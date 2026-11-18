"""同原件共享一次源扫描的核心调度。

设计要点
========
* 一个 source 同一时刻最多有一个活跃 ScanController（一代扫描，目录
  scans/<source_id>/gen<N>/）。同原件的在途任务都作为 Subscriber 挂到
  该控制器上，扫描线程只对原件做一次顺序读取。
* 扫描线程把校验通过的每一行赋予单调递增的**行序列号**，批量追加写入本代
  spool（磁盘日志）；一批落盘并 flush 完成后，才在条件锁内发布
  ``published_seq``（已可安全读取的行数）。序列号发布与订阅者的加入/读取
  在同一把条件锁内对齐，是“不重不漏”的根基。
* 订阅者只从 spool **拉取**自己尚未处理的序列号区间：加入时 ``next_seq=1``，
  已发布的行全部在 spool 文件里（含加入之前的行），之后发布的行经条件变量
  唤醒继续读取。全程没有内存快照、没有在线推送、没有第二数据源，因此
  任何时刻加入都恰好按原顺序处理每行一次，原件中的重复行原样保留，
  且绝不二次打开原件。
* 只有整份原件解析通过后，订阅者才会把暂存原子改名为结果文件并置
  completed；任一行格式错误或扫描异常，本代所有未完任务 failed，
  不提供部分结果。
* 取消只影响本任务；最后一个活跃任务取消时，扫描线程在下一个检查点真正
  停止并清空本代暂存目录后，任务才以 cancelled 落库。
* 收尾期间允许受理新任务：管理器为其建立新一代扫描，旧扫描的清理只动
  旧代目录，互不干扰。
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import config
from .csvutil import HEADER, CsvFormatError, parse_row, row_matches, validate_header
from .db import Database

# 取消/结束事件的轮询粒度，保证“及时受理”
POLL_INTERVAL = 0.05
# spool 落盘批量：行数或字节数任一达到即 flush。
# 一批先完整 flush 再发布序列号，订阅者永远不会读到半行。
SPOOL_FLUSH_BYTES = 256 * 1024
SPOOL_FLUSH_ROWS = 32


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class TaskState:
    """一个交付任务的内存态（DB 是持久镜像）。"""

    task_id: str
    source_id: str
    stations: frozenset[str] | None  # None / 空集 = 全部站号
    temp_min: int
    temp_max: int
    status: str = "pending"
    matched_rows: int = 0
    error: str | None = None
    result_path: str | None = None
    created_at: str = field(default_factory=_now)
    finished: threading.Event = field(default_factory=threading.Event)

    def public(self) -> dict:
        return {
            "task_id": self.task_id,
            "source_id": self.source_id,
            "stations": sorted(self.stations) if self.stations else None,
            "temp_min": self.temp_min,
            "temp_max": self.temp_max,
            "status": self.status,
            "matched_rows": self.matched_rows,
            "error": self.error,
            "result_path": self.result_path,
            "result_available": self.status == "completed",
            "created_at": self.created_at,
        }


class Subscriber:
    """订阅一次扫描的单个任务：从 spool 按序列号拉取、筛行并落盘结果。"""

    def __init__(self, state: TaskState, controller: "ScanController", db: Database):
        self.state = state
        self.controller = controller
        self.db = db
        self.thread = threading.Thread(
            target=self._run, name=f"sub-{state.task_id[:8]}", daemon=True
        )
        self._io_lock = threading.Lock()  # 保护暂存文件的写/改名/删除
        self._staging = controller.dir / f"{state.task_id}.staging.csv"
        self._file = None
        # 下一条需要处理的行序列号（从 1 开始）。加入控制器时所有
        # seq < next_seq 的行都已完整落在 spool 中，不会漏也不会重。
        self.next_seq = 1
        self.cancel_requested = False
        self.retired = False

    # -- 生命周期 ----------------------------------------------------------
    def start(self) -> None:
        self.thread.start()

    def request_cancel(self) -> None:
        self.cancel_requested = True
        self.state.status = "cancelling"
        self.db.update_task(self.state.task_id, status="cancelling", now=_now())
        with self.controller.cond:
            self.controller.cond.notify_all()

    def retire(self) -> None:
        """线程退出时的一次性收尾：关文件并按最终状态处理暂存。"""
        if self.retired:
            return
        self.retired = True
        with self._io_lock:
            if self._file is not None:
                try:
                    self._file.close()
                except Exception:
                    pass
                self._file = None
            if self.state.status == "completed":
                return  # 已原子改名，无暂存可清
            if self._staging.exists():
                try:
                    self._staging.unlink()
                except OSError:
                    pass

    # -- 主循环 ------------------------------------------------------------
    def _run(self) -> None:
        ctl = self.controller
        try:
            self._open_staging()
            # spool 在控制器创建时即 touch，订阅者可随时以 rb 打开；
            # 只读到已发布序列号对应的字节，绝不读到半行。
            with open(ctl.spool_path, "rb") as spool:
                while True:
                    with ctl.cond:
                        while (
                            ctl.state == "running"
                            and ctl.published_seq < self.next_seq
                            and not self.cancel_requested
                        ):
                            ctl.cond.wait(timeout=POLL_INTERVAL)
                        state = ctl.state
                        available = ctl.published_seq

                    if self.cancel_requested and state == "running":
                        self._finish_cancelled()
                        return

                    # 拉取本订阅者欠着的所有已发布行，每行恰好处理一次
                    while self.next_seq <= available:
                        line = spool.readline()
                        if not line:  # 防御：序列号与文件内容理应一致
                            break
                        self._handle_line(line.decode("utf-8"))
                        self.next_seq += 1

                    if state == "complete":
                        self._finish_complete()
                        return
                    if state == "error":
                        self._finish_failed(ctl.error or "扫描失败")
                        return
                    if state == "aborted":
                        # 用户主动取消记 cancelled；进程关闭导致的中止记 interrupted
                        if not self.cancel_requested and ctl.manager.shutting_down:
                            self._finish_interrupted("服务关闭，任务中断")
                        else:
                            self._finish_cancelled()
                        return
                    # state == "running" 且暂无新行：回到条件变量继续等
        except Exception as exc:  # 订阅者自身 IO 异常也算任务失败
            self._finish_failed(f"任务处理异常: {exc!r}")
        finally:
            self.retire()
            with ctl.cond:
                ctl.subscribers.discard(self)
                if not ctl.subscribers:
                    ctl.all_left.set()
                ctl.cond.notify_all()
            ctl.manager._on_subscriber_left(ctl)

    def _open_staging(self) -> None:
        self._file = open(self._staging, "w", encoding="utf-8", newline="")
        self._file.write(",".join(HEADER) + "\r\n")

    def _handle_line(self, line: str) -> None:
        """对一行执行本任务的筛选并写暂存。行内容已由扫描线程校验。"""
        station, _dt, temperature = parse_row(line, -1)
        if row_matches(
            station,
            temperature,
            set(self.state.stations) if self.state.stations else None,
            self.state.temp_min,
            self.state.temp_max,
        ):
            with self._io_lock:
                if self._file is not None:
                    self._file.write(line if line.endswith("\n") else line + "\r\n")
                    self.state.matched_rows += 1

    # -- 终态处理 ----------------------------------------------------------
    def _finish_complete(self) -> None:
        if self.state.finished.is_set():
            return
        if self.cancel_requested:
            self._finish_cancelled()
            return
        result = config.RESULTS_DIR / f"{self.state.task_id}.csv"
        with self._io_lock:
            if self._file is not None:
                self._file.flush()
                self._file.close()
                self._file = None
            self._staging.replace(result)  # 同目录原子改名
        self.state.status = "completed"
        self.state.result_path = str(result)
        self.db.update_task(
            self.state.task_id,
            status="completed",
            result_path=str(result),
            matched_rows=self.state.matched_rows,
            now=_now(),
        )
        self.state.finished.set()

    def _finish_failed(self, error: str) -> None:
        if self.state.finished.is_set():
            return
        self.state.status = "failed"
        self.state.error = error
        self.db.update_task(
            self.state.task_id, status="failed", error=error, now=_now()
        )
        self.state.finished.set()

    def _finish_cancelled(self) -> None:
        if self.state.finished.is_set():
            return
        self.state.status = "cancelled"
        self.state.error = None
        self.db.update_task(
            self.state.task_id, status="cancelled", error=None, now=_now()
        )
        self.state.finished.set()

    def _finish_interrupted(self, error: str) -> None:
        if self.state.finished.is_set():
            return
        self.state.status = "interrupted"
        self.state.error = error
        self.db.update_task(
            self.state.task_id, status="interrupted", error=error, now=_now()
        )
        self.state.finished.set()


class ScanController:
    """一代扫描：一个扫描线程 + 若干拉取式订阅者 + 独立暂存目录。"""

    def __init__(
        self,
        manager: "ScanManager",
        source_id: str,
        generation: int,
        source_path: Path,
        row_delay_ms: int,
    ):
        self.manager = manager
        self.db = manager.db
        self.source_id = source_id
        self.generation = generation
        self.dir = config.SCANS_DIR / source_id / f"gen{generation}"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.spool_path = self.dir / "spool.csv"
        # 预创建 spool，保证订阅者线程任何时刻都能以 rb 打开
        self.spool_path.touch()
        self.source_path = source_path
        self.row_delay_ms = row_delay_ms

        self.cond = threading.Condition()
        self.subscribers: set[Subscriber] = set()
        # running -> complete / error / aborted
        self.state = "running"
        self.error: str | None = None
        # 已发布序列号：spool 中前 published_seq 行已完整 flush，可安全读取
        self.published_seq = 0
        self.all_left = threading.Event()
        self.terminated = threading.Event()
        self.scanned_rows = 0  # 已从原件读取的数据行数（进度观测）
        self._pending: list[bytes] = []
        self._pending_bytes = 0
        self._pending_rows = 0
        self._stop_requested = False
        self.thread = threading.Thread(target=self._run, name=f"scan-{source_id[:8]}", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def request_stop(self) -> None:
        """最后一个订阅者离开时调用，请求扫描线程尽快停止。"""
        with self.cond:
            self._stop_requested = True
            self.cond.notify_all()

    # -- 扫描线程 ----------------------------------------------------------
    def _run(self) -> None:
        try:
            self.db.set_source_status(self.source_id, "scanning")
            self._scan()
        except CsvFormatError as exc:
            self._enter_error(str(exc))
        except Exception as exc:
            self._enter_error(f"扫描失败: {exc!r}")
        finally:
            self._terminate()

    def _scan(self) -> None:
        with open(self.source_path, "r", encoding="utf-8", newline="") as src, open(
            self.spool_path, "ab"
        ) as spool:
            header_line = src.readline()
            if not header_line:
                raise CsvFormatError("空文件，缺少表头")
            validate_header(header_line)

            line_no = 1
            for raw_line in src:
                line_no += 1
                self.scanned_rows = line_no - 1
                # 取消检查点：最后一个订阅者离开后尽快停下
                with self.cond:
                    if self._stop_requested:
                        self._enter_aborted()
                        return
                # 校验：整份原件全部合法才允许产出结果
                parse_row(raw_line, line_no)

                self._buffer_and_maybe_publish(raw_line, spool)

                if self.row_delay_ms > 0 and line_no % config.SCAN_DELAY_EVERY_ROWS == 0:
                    self._interruptible_sleep(self.row_delay_ms / 1000.0)

            self._publish_batch(spool)  # EOF 末批：先落盘再发布 complete

        with self.cond:
            self.state = "complete"
            self.cond.notify_all()
        self.db.set_source_status(self.source_id, "validated")

    def _interruptible_sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        with self.cond:
            while time.monotonic() < deadline:
                if self._stop_requested:
                    return
                self.cond.wait(timeout=min(POLL_INTERVAL, deadline - time.monotonic()))

    def _buffer_and_maybe_publish(self, raw_line: str, spool) -> None:
        # spool 以二进制追加，统一编码为 UTF-8，保留原件原始换行符
        encoded = raw_line.encode("utf-8")
        self._pending.append(encoded)
        self._pending_bytes += len(encoded)
        self._pending_rows += 1
        if (
            self._pending_bytes >= SPOOL_FLUSH_BYTES
            or self._pending_rows >= SPOOL_FLUSH_ROWS
        ):
            self._publish_batch(spool)

    def _publish_batch(self, spool) -> None:
        """先把整批完整 flush 到 spool，再在条件锁内发布序列号。

        发布之后，这批行对所有订阅者（含即将加入者）都可经 spool 读取；
        发布之前没有任何订阅者能看到它们。行的可见性只在这里切换一次，
        因此加入时刻与扫描进度任意交错都不会丢行或重行。
        """
        if not self._pending:
            return
        chunk = b"".join(self._pending)
        rows_in_batch = self._pending_rows
        spool.write(chunk)
        spool.flush()
        self._pending.clear()
        self._pending_bytes = 0
        self._pending_rows = 0
        with self.cond:
            self.published_seq += rows_in_batch
            self.cond.notify_all()

    def _enter_aborted(self) -> None:
        with self.cond:
            self.state = "aborted"
            self.cond.notify_all()

    def _enter_error(self, error: str) -> None:
        self.error = error
        with self.cond:
            self.state = "error"
            self.cond.notify_all()
        self.db.set_source_status(self.source_id, "invalid")

    def _terminate(self) -> None:
        """扫描线程终态收尾：等订阅者散去，删除本代暂存，释放目录。"""
        # 唤醒所有等待者，让它们按终态结束
        with self.cond:
            self.cond.notify_all()
        self.all_left.wait()
        # 此刻已无订阅者，清理只涉及本代目录
        for child in self.dir.iterdir():
            try:
                if child.is_file():
                    child.unlink()
                else:
                    for f in child.iterdir():
                        f.unlink()
                    child.rmdir()
            except OSError:
                pass
        try:
            self.dir.rmdir()
            parent = self.dir.parent
            try:
                parent.rmdir()  # source 下没有其他代时一并删除
            except OSError:
                pass
        except OSError:
            pass
        self.terminated.set()
        self.manager._on_controller_terminated(self)


class ScanManager:
    """对外门面：受理任务、复用/新建扫描代、处理取消与重启恢复。"""

    def __init__(self, db: Database):
        self.db = db
        self._lock = threading.RLock()
        # source_id -> 最新一代控制器（终态后保留到 terminated 回调摘除）
        self._controllers: dict[str, ScanController] = {}
        self._generations: dict[str, int] = {}
        self.tasks: dict[str, TaskState] = {}
        self.shutting_down = False

    # -- 任务受理（不等待扫描） --------------------------------------------
    def create_task(
        self,
        source_id: str,
        stations: list[str] | None,
        temp_min: int,
        temp_max: int,
    ) -> TaskState:
        source = self.db.get_source(source_id)
        if source is None:
            raise KeyError(source_id)
        if temp_min > temp_max:
            raise ValueError("temp_min 不能大于 temp_max")

        task_id = uuid.uuid4().hex
        station_set = frozenset(stations) if stations else frozenset()
        state = TaskState(
            task_id=task_id,
            source_id=source_id,
            stations=station_set or None,
            temp_min=temp_min,
            temp_max=temp_max,
        )
        self.db.insert_task(
            task_id,
            source_id,
            json.dumps(sorted(station_set)),
            temp_min,
            temp_max,
            state.created_at,
        )
        self.tasks[task_id] = state

        with self._lock:
            ctl = self._controllers.get(source_id)
            # 旧扫描在终态/停止收尾中也不影响：开新一代，目录彼此隔离。
            # 检查与挂载在控制器同一把条件锁内完成，杜绝挂到即将停止的旧代。
            if ctl is None or ctl.state != "running":
                ctl = self._new_controller(source_id, int(source["row_delay_ms"]))
            sub = Subscriber(state, ctl, self.db)
            with ctl.cond:
                if ctl.state != "running" or ctl._stop_requested:
                    ctl = self._new_controller(source_id, int(source["row_delay_ms"]))
                    sub = Subscriber(state, ctl, self.db)
                # 挂载瞬间：published_seq 之前的行全部已在 spool，
                # 之后的行发布时会唤醒本订阅者，序列号区间首尾相接。
                ctl.subscribers.add(sub)
                ctl.cond.notify_all()
            state.status = "running"
            self.db.update_task(task_id, status="running", now=_now())
            sub.start()
        return state

    def _new_controller(self, source_id: str, row_delay_ms: int) -> ScanController:
        gen = self._generations.get(source_id, 0) + 1
        self._generations[source_id] = gen
        source_path = config.SOURCES_DIR / f"{source_id}.csv"
        ctl = ScanController(self, source_id, gen, source_path, row_delay_ms)
        self._controllers[source_id] = ctl
        ctl.start()
        return ctl

    # -- 取消 --------------------------------------------------------------
    def cancel_task(self, task_id: str, wait_timeout: float = 30.0) -> TaskState:
        state = self.tasks.get(task_id)
        if state is None:
            raise KeyError(task_id)
        if state.finished.is_set():
            return state

        ctl = self._controllers.get(state.source_id)
        sub = self._find_subscriber(ctl, task_id)
        is_last = False
        if sub is not None:
            sub.request_cancel()
            with ctl.cond:
                # 活跃 = 仍在订阅集合中、且未请求取消的订阅者
                # （已进入终态的订阅者会在 finally 中自行从集合移除）
                active = [
                    s for s in ctl.subscribers if not s.cancel_requested
                ]
                is_last = len(active) == 0
            if is_last:
                ctl.request_stop()

        # 任务终态先落定
        if not state.finished.wait(timeout=wait_timeout):
            return state
        # 最后一个任务：必须等扫描实际停止、暂存清除后才报 cancelled
        if is_last and ctl is not None and ctl.state != "complete":
            ctl.terminated.wait(timeout=wait_timeout)
        return state

    def _find_subscriber(
        self, ctl: ScanController | None, task_id: str
    ) -> Subscriber | None:
        if ctl is None:
            return None
        with ctl.cond:
            for sub in ctl.subscribers:
                if sub.state.task_id == task_id:
                    return sub
        return None

    # -- 控制器回调 ---------------------------------------------------------
    def _on_subscriber_left(self, ctl: ScanController) -> None:
        with ctl.cond:
            if not ctl.subscribers and ctl.state == "running":
                # 兜底：正常路径由 cancel_task 触发，这里防止漏网
                ctl.request_stop()

    def _on_controller_terminated(self, ctl: ScanController) -> None:
        with self._lock:
            if self._controllers.get(ctl.source_id) is ctl:
                del self._controllers[ctl.source_id]

    # -- 查询 ---------------------------------------------------------------
    def get_task(self, task_id: str) -> TaskState | None:
        return self.tasks.get(task_id)

    def get_scanned_rows(self, source_id: str) -> int:
        ctl = self._controllers.get(source_id)
        return ctl.scanned_rows if ctl is not None else 0

    # -- 重启恢复 -----------------------------------------------------------
    def recover_on_startup(self) -> None:
        """清掉上一代遗留暂存；未完任务已由 DB 层统一标记 interrupted。"""
        ids = self.db.interrupt_active_tasks()
        for tid in ids:
            stale = config.RESULTS_DIR / f"{tid}.csv"
            # 中断任务不保留结果（completed 的不在 ids 中，不受影响）
            if stale.exists():
                try:
                    stale.unlink()
                except OSError:
                    pass
        if config.SCANS_DIR.exists():
            import shutil

            shutil.rmtree(config.SCANS_DIR, ignore_errors=True)
        config.SCANS_DIR.mkdir(parents=True, exist_ok=True)

    def shutdown(self, timeout: float = 10.0) -> None:
        """进程关闭：停止全部在途扫描并等待线程退出，避免线程访问已关闭资源。"""
        self.shutting_down = True
        with self._lock:
            controllers = list(self._controllers.values())
        for ctl in controllers:
            ctl.request_stop()
        deadline = time.monotonic() + timeout
        for ctl in controllers:
            remaining = max(0.0, deadline - time.monotonic())
            ctl.terminated.wait(timeout=remaining)
