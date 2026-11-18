"""共享扫描与内存预算的专项测试。"""
from __future__ import annotations

import os
import threading
import time

from helpers import create_task, make_csv, upload, wait_terminal


def test_source_file_opened_once_for_many_tasks(client, monkeypatch):
    """同原件 5 个任务（含途中加入）只能触发一次对原件的打开读取。"""
    import builtins

    opened: list[tuple[str, str]] = []
    real_open = builtins.open

    def counting_open(file, mode="r", *args, **kwargs):
        handle = real_open(file, mode, *args, **kwargs)
        try:
            path = os.fspath(file)
            if path.endswith(".csv") and "/sources/" in path and "r" in mode:
                opened.append((path, threading.current_thread().name))
        except (TypeError, ValueError):
            pass
        return handle

    monkeypatch.setattr(builtins, "open", counting_open)

    content = make_csv(300)
    sid = upload(client, content, delay_ms=2).json()["source_id"]
    tids = [
        create_task(client, sid, stations=[s]).json()["task_id"]
        for s in ("STA01", "STA02", "STA03")
    ]
    import time
    time.sleep(0.1)  # 确保后两个任务在扫描途中加入
    tids.append(create_task(client, sid, stations=["STA01"]).json()["task_id"])
    tids.append(create_task(client, sid).json()["task_id"])

    for tid in tids:
        assert wait_terminal(client, tid)["status"] == "completed"

    assert len(opened) == 1, f"原件被打开 {len(opened)} 次，期望 1 次"
    assert opened[0][1].startswith("scan-")


def test_concurrent_acceptance_during_teardown(client):
    """收尾窗口并发受理：新一代扫描互不干扰，全部完整交付。"""
    sid = upload(client, make_csv(500), delay_ms=3).json()["source_id"]
    first = create_task(client, sid).json()["task_id"]
    import time
    time.sleep(0.1)

    task_ids: list[str] = []
    errors: list[Exception] = []

    def worker(i: int) -> None:
        try:
            resp = create_task(client, sid, stations=[f"STA{i:03d}"])
            task_ids.append(resp.json()["task_id"])
        except Exception as exc:  # pragma: no cover - 并发不应出错
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    client.post(f"/tasks/{first}/cancel")
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(task_ids) == 8
    for tid in task_ids:
        info = wait_terminal(client, tid)
        assert info["status"] == "completed"
        body = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
        assert body[0] == "station,utc_time,temperature"


def test_repeated_late_joiners_never_lose_rows(client):
    """扫描途中反复加入新任务，每个迟到者都必须完整补齐（覆盖 pending 窗口竞态）。"""
    import time

    content = make_csv(300)
    sid = upload(client, content, delay_ms=2).json()["source_id"]
    expected_all = content.decode().splitlines()[1:]

    task_ids: list[str] = []
    first = create_task(client, sid).json()["task_id"]
    task_ids.append(first)
    # 扫描约每 2 行就加入一个新任务，制造大量“部分 spool + 部分 pending”的补读时刻
    for _ in range(20):
        time.sleep(0.004)
        task_ids.append(create_task(client, sid).json()["task_id"])

    for tid in task_ids:
        info = wait_terminal(client, tid)
        assert info["status"] == "completed", info
        body = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
        # 无条件任务必须得到与原件完全一致的全部行（顺序、重复行保留）
        assert body == ["station,utc_time,temperature"] + expected_all


def test_query_endpoints_return_while_scan_in_flight(client):
    """查询接口不等待扫描：扫描在途时 source/task 列表立即可读。"""
    sid = upload(client, make_csv(400), delay_ms=5).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]

    assert client.get("/sources").status_code == 200
    assert client.get("/tasks").status_code == 200
    detail = client.get(f"/tasks/{tid}").json()
    assert detail["status"] in ("running", "cancelling", "completed")
    assert "scanned_rows" in detail
    wait_terminal(client, tid)


def test_late_joiner_gets_row_validated_but_unpublished_at_join(client, monkeypatch):
    """回归：加入瞬间，已校验但尚未发布的行不能丢（原缺陷的确定性复现）。

    旧实现在“行已交给旧任务、新订阅者快照已取、行尚未进 spool”的窗口里
    会让新任务永久少一行。新实现以序列号+spool 单一事实源交接，该行必须
    在发布后由新订阅者恰好拉取一次。
    """
    import app.scan as scan_mod

    target_row = 50
    gate = threading.Event()
    real_publish = scan_mod.ScanController._publish_batch
    triggered = threading.Event()

    def gated_publish(self, spool):
        if self.scanned_rows >= target_row and not triggered.is_set():
            triggered.set()
            gate.wait(timeout=10)  # 停在“已扫描、未发布”窗口
        return real_publish(self, spool)

    monkeypatch.setattr(scan_mod.ScanController, "_publish_batch", gated_publish)

    content = make_csv(300)
    expected_all = content.decode().splitlines()[1:]
    sid = upload(client, content, delay_ms=0).json()["source_id"]
    first = create_task(client, sid).json()["task_id"]

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if client.get(f"/tasks/{first}").json().get("scanned_rows", 0) >= target_row:
            break
        time.sleep(0.005)
    assert triggered.wait(timeout=5)
    # 在发布窗口内受理新任务：它的 next_seq=1，发布后必须补齐全部已扫行
    late = create_task(client, sid).json()["task_id"]
    time.sleep(0.2)
    gate.set()

    for tid in (first, late):
        info = wait_terminal(client, tid)
        assert info["status"] == "completed", info
        body = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
        assert body == ["station,utc_time,temperature"] + expected_all


def test_join_at_eof_last_batch_no_duplicate_or_missing(client, monkeypatch):
    """回归：EOF 末次发布与加入交错，末批行不重不漏。"""
    import app.scan as scan_mod

    joined = threading.Event()
    real_publish = scan_mod.ScanController._publish_batch
    hooked = threading.Event()

    def gated_publish(self, spool):
        if self.scanned_rows >= 20 and not hooked.is_set():
            hooked.set()
            joined.wait(timeout=10)
            time.sleep(0.1)
        return real_publish(self, spool)

    monkeypatch.setattr(scan_mod.ScanController, "_publish_batch", gated_publish)

    content = make_csv(20)
    expected_all = content.decode().splitlines()[1:]
    sid = upload(client, content, delay_ms=2).json()["source_id"]
    first = create_task(client, sid).json()["task_id"]
    assert hooked.wait(timeout=10)
    late = create_task(client, sid).json()["task_id"]
    joined.set()

    for tid in (first, late):
        info = wait_terminal(client, tid)
        assert info["status"] == "completed", info
        body = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
        assert body == ["station,utc_time,temperature"] + expected_all


def test_chaotic_late_joiners_every_task_exactly_once_per_row(client):
    """压力交错：扫描途中在任意相位反复加入，每个任务对每行恰好处理一次。

    无条件任务的结果必须与原件逐行一致（顺序与重复行都保留）；
    同时原件自始至终只被扫描线程打开一次。
    """
    import builtins

    real_open = builtins.open
    opened: list[str] = []

    def counting_open(file, mode="r", *args, **kwargs):
        handle = real_open(file, mode, *args, **kwargs)
        try:
            path = os.fspath(file)
            if path.endswith(".csv") and "/sources/" in path and "r" in mode:
                opened.append(path)
        except (TypeError, ValueError):
            pass
        return handle

    import app.scan as scan_mod
    builtins.open = counting_open
    try:
        content = make_csv(400, dup=True)
        expected_all = content.decode().splitlines()[1:]
        sid = upload(client, content, delay_ms=1).json()["source_id"]
        task_ids = [create_task(client, sid).json()["task_id"]]
        for _ in range(25):
            time.sleep(0.003)
            task_ids.append(create_task(client, sid).json()["task_id"])

        for tid in task_ids:
            info = wait_terminal(client, tid)
            assert info["status"] == "completed", info
            body = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
            assert body == ["station,utc_time,temperature"] + expected_all
    finally:
        builtins.open = real_open

    assert len(opened) == 1, f"原件被打开 {len(opened)} 次，期望 1 次"


def test_late_joiner_with_filter_preserves_duplicate_matches(client, monkeypatch):
    """带筛选条件的迟到任务：匹配行（含原件重复行）按原序补齐一次。"""
    import app.scan as scan_mod

    gate = threading.Event()
    real_publish = scan_mod.ScanController._publish_batch
    triggered = threading.Event()

    def gated_publish(self, spool):
        if self.scanned_rows >= 30 and not triggered.is_set():
            triggered.set()
            gate.wait(timeout=10)
        return real_publish(self, spool)

    monkeypatch.setattr(scan_mod.ScanController, "_publish_batch", gated_publish)

    content = make_csv(120, dup=True)
    sid = upload(client, content, delay_ms=0).json()["source_id"]
    first = create_task(client, sid, stations=["STA01"]).json()["task_id"]
    assert triggered.wait(timeout=10)
    late = create_task(client, sid, stations=["STA01"]).json()["task_id"]
    gate.set()

    expected = [
        ln for ln in content.decode().splitlines()[1:] if ln.split(",")[0] == "STA01"
    ]
    for tid in (first, late):
        info = wait_terminal(client, tid)
        assert info["status"] == "completed", info
        body = client.get(f"/tasks/{tid}/result").content.decode().splitlines()[1:]
        assert body == expected
    # 原件首行重复，且首行属于 STA01，结果前两行必须相同
    body = client.get(f"/tasks/{late}/result").content.decode().splitlines()
    assert body[1] == body[2]
