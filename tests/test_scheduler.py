"""调度层自测：并发名额、等候不占线程、后台恢复扫描、上传持久化门禁、
扫描/上传/页面交错时单执行者。

运行（需 Flask）：
  PYTHONPATH=.pylibs:app python3 -I -m unittest tests.test_scheduler -v
"""

from __future__ import annotations

import csv
import io
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import uuid

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (os.path.join(REPO_ROOT, "app"),
           os.path.join(REPO_ROOT, ".pylibs")):
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

import app as webapp  # noqa: E402
import join_engine as je  # noqa: E402


def _csv_bytes(header: list, rows: list) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _wait_until(predicate, timeout: float = 10.0, message: str = "等待超时") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(message)


class SchedulerTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        webapp.JOBS_ROOT = os.path.join(self.tmp, "jobs")
        os.makedirs(webapp.JOBS_ROOT, exist_ok=True)
        self._old_limits = (webapp.MAX_CONCURRENT_JOBS,
                            webapp.MAX_RECOVERED_RUNNING)
        self._reset_globals(recovered=True)
        webapp.app.config["TESTING"] = True
        self.client = webapp.app.test_client()

    def tearDown(self) -> None:
        # 释放所有阻塞中的假作业，等执行线程退出，避免跨测试污染
        for event in getattr(self, "_events", {}).values():
            event.set()
        for thread in list(webapp._threads.values()):
            thread.join(timeout=10)
        webapp.MAX_CONCURRENT_JOBS, webapp.MAX_RECOVERED_RUNNING = self._old_limits
        self._reset_globals(recovered=True)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _reset_globals(self, recovered: bool) -> None:
        worker = getattr(webapp, "_recovery_thread", None)
        if worker is not None and worker.is_alive():
            worker.join(timeout=10)
        with webapp._jobs_lock:
            webapp._threads.clear()
            webapp._running_origin.clear()
            webapp._queued_new.clear()
            webapp._queued_recovered.clear()
            webapp._queued_origin.clear()
            webapp._tracked.clear()
            webapp._uploading.clear()
            webapp._recovered = recovered
            webapp._recovery_started = False
            webapp._recovery_thread = None

    def _upload(self, tasks, machines):
        data = {
            "tasks_csv": (io.BytesIO(_csv_bytes(["任务号", "层摘要"], tasks)),
                          "tasks.csv"),
            "machines_csv": (io.BytesIO(
                _csv_bytes(["来源构建机", "层摘要"], machines)), "machines.csv"),
        }
        return self.client.post(
            "/upload", data=data, content_type="multipart/form-data",
            follow_redirects=False)

    def _job_id(self, resp) -> str:
        self.assertEqual(resp.status_code, 302, resp.data[:300])
        return resp.headers["Location"].rstrip("/").split("/")[-1]

    def _blocking_run_join(self):
        """返回 (fake_run_join, events)：每个作业在 done 事件上阻塞。"""
        events = {}
        calls = {}
        lock = threading.Lock()

        def fake(job_dir: str) -> dict:
            job_id = os.path.basename(job_dir)
            state = je.load_state(job_dir)
            # 与真实引擎一致：接单未开跑的 queued 先推进到 partitioning
            if state.get("phase") == "queued":
                state["phase"] = "partitioning"
                je.save_state(job_dir, state)
            else:
                state = je.load_state(job_dir)
            with lock:
                calls[job_id] = calls.get(job_id, 0) + 1
                event = events.setdefault(job_id, threading.Event())
            event.wait(timeout=30)
            state = je.load_state(job_dir)
            state["phase"] = "done"
            je.save_state(job_dir, state)
            return state

        self._events = events
        self._events_lock = lock
        return fake, events, calls

    def _release_all_until_idle(self, expected: set) -> None:
        """持续放行（包括运行中作业结束后才补位启动的新作业），直到全部结束。"""
        deadline = time.time() + 30
        while time.time() < deadline:
            with self._events_lock:
                current = list(self._events.values())
            for event in current:
                event.set()
            if not webapp._tracked:
                break
            time.sleep(0.02)
        _wait_until(lambda: not webapp._tracked, message="作业未全部结束")
        _wait_until(lambda: set(self._events) == expected,
                    message="有作业未被执行")
    def _make_disk_job(self, phase: str = "joining", n: int = 20) -> str:
        # 与网页上传一致的 uuid hex 目录名（不含下划线等非法字符）
        job_id = uuid.uuid4().hex
        jdir = os.path.join(webapp.JOBS_ROOT, job_id)
        os.makedirs(jdir)
        with open(os.path.join(jdir, "tasks.csv"), "w", encoding="utf-8",
                  newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["任务号", "层摘要"])
            for i in range(n):
                w.writerow([f"T{i}", f"d{i % 5}"])
        with open(os.path.join(jdir, "machines.csv"), "w", encoding="utf-8",
                  newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["来源构建机", "层摘要"])
            for i in range(n):
                w.writerow([f"M{i}", f"d{i % 5}"])
        je.save_state(jdir, {
            "phase": phase, "total_pairs": 0, "buckets": {},
            "skipped_rows": {"tasks": 0, "machines": 0}})
        return job_id


class ConcurrencyTests(SchedulerTestBase):
    def test_at_most_two_running_and_queue_holds_no_threads(self):
        webapp.MAX_CONCURRENT_JOBS = 2
        webapp.MAX_RECOVERED_RUNNING = 1
        fake, events, calls = self._blocking_run_join()
        old = webapp.run_join
        webapp.run_join = fake
        try:
            job_ids = [self._job_id(self._upload(
                [(f"T{i}", "d1")], [(f"M{i}", "d1")])) for i in range(4)]
            _wait_until(lambda: len(events) == 2)

            self.assertEqual(len(webapp._threads), 2)
            self.assertEqual(len(webapp._queued_new), 2)
            # 等候作业没有执行线程
            for job_id in webapp._queued_new:
                self.assertNotIn(job_id, webapp._threads)

            statuses = [self.client.get(
                f"/jobs/{j}/status").get_json() for j in job_ids]
            self.assertEqual(sum(1 for s in statuses if s["running"]), 2)
            self.assertEqual(sum(1 for s in statuses if s["queued"]), 2)

            # 页面同时展示"运行中"与"等候执行"
            page = self.client.get("/").data.decode("utf-8")
            self.assertIn("运行中", page)
            self.assertIn("等候执行", page)
            queued_job = next(j for j, s in zip(job_ids, statuses) if s["queued"])
            detail = self.client.get(f"/jobs/{queued_job}").data.decode("utf-8")
            self.assertIn("已接单，等候执行", detail)

            # 逐个放行；每放行一个，空位立即补给下一个等候作业
            self._release_all_until_idle(set(job_ids))
            self.assertTrue(all(v == 1 for v in calls.values()))
        finally:
            webapp.run_join = old

    def test_recovered_jobs_take_one_slot_new_job_gets_the_other(self):
        webapp.MAX_CONCURRENT_JOBS = 2
        webapp.MAX_RECOVERED_RUNNING = 1
        fake, events, calls = self._blocking_run_join()
        old = webapp.run_join
        webapp.run_join = fake
        try:
            recovered = [self._make_disk_job("joining") for _ in range(3)]
            webapp._recover_on_startup()
            _wait_until(lambda: len(events) == 1)
            self.assertEqual(len(webapp._threads), 1)
            self.assertEqual(len(webapp._queued_recovered), 2)
            # 新上传作业立即使用另一个空位，不与恢复名额争抢
            new_job = self._job_id(self._upload([("T", "d")], [("M", "d")]))
            _wait_until(lambda: len(webapp._threads) == 2)
            self.assertIn(new_job, webapp._threads)
            self.assertEqual(
                sum(1 for o in webapp._running_origin.values()
                    if o == "recovered"), 1)

            # 放行运行中的作业后，等候的恢复作业应逐个补位执行
            self._release_all_until_idle(set(recovered) | {new_job})
            self.assertEqual(set(calls), set(recovered) | {new_job})
            self.assertTrue(all(v == 1 for v in calls.values()))
        finally:
            webapp.run_join = old

    def test_interleaved_scan_has_single_executor_per_job(self):
        webapp.MAX_CONCURRENT_JOBS = 2
        webapp.MAX_RECOVERED_RUNNING = 1
        fake, events, calls = self._blocking_run_join()
        old = webapp.run_join
        webapp.run_join = fake
        try:
            job_ids = [self._make_disk_job("partitioning") for _ in range(6)]
            threads = [threading.Thread(target=webapp._recover_on_startup)
                       for _ in range(3)]
            for t in threads:
                t.start()
            # 扫描期间同时进行上传与页面访问
            uploaded = self._job_id(self._upload([("T", "d")], [("M", "d")]))
            self.client.get("/")
            for t in threads:
                t.join(timeout=10)
            # 每个作业最多只有一次执行
            self._release_all_until_idle(set(job_ids) | {uploaded})
            self.assertTrue(all(v == 1 for v in calls.values()))
            self.assertEqual(set(calls) - {uploaded}, set(job_ids))
        finally:
            webapp.run_join = old


class RecoveryScanTests(SchedulerTestBase):
    def test_scan_does_not_block_page_access(self):
        started = threading.Event()
        release = threading.Event()

        def slow_scan():
            started.set()
            release.wait(timeout=10)

        old = webapp._recover_on_startup
        webapp._recover_on_startup = slow_scan
        webapp._recovered = False
        webapp._recovery_started = False
        try:
            result = {}
            request_thread = threading.Thread(
                target=lambda: result.setdefault(
                    "resp", self.client.get("/")))
            request_thread.start()
            self.assertTrue(started.wait(timeout=5), "后台扫描未启动")
            request_thread.join(timeout=5)
            self.assertFalse(request_thread.is_alive(), "页面请求被扫描阻塞")
            self.assertEqual(result["resp"].status_code, 200)
            release.set()
        finally:
            release.set()
            worker = webapp._recovery_thread
            if worker is not None:
                worker.join(timeout=10)
            webapp._recover_on_startup = old
            webapp._recovered = True

    def test_queued_job_is_enqueued_after_restart(self):
        """已接单但未开跑的作业（phase=queued）重启后也必须执行。"""
        job_id = self._make_disk_job("queued", n=30)
        webapp._recovered = False
        webapp._recover_on_startup()
        jdir = os.path.join(webapp.JOBS_ROOT, job_id)
        _wait_until(
            lambda: je.load_state(jdir).get("phase") == "done",
            timeout=30, message="queued 作业重启后未执行")
        state = je.load_state(jdir)
        self.assertTrue(os.path.exists(os.path.join(jdir, "pairs.csv")))
        self.assertGreater(state["total_pairs"], 0)


class UploadDurabilityTests(SchedulerTestBase):
    def test_stale_staging_cleaned_and_ignored_by_scan(self):
        # 模拟上次进程在落盘中途崩溃：留下点开头的暂存目录
        stale = os.path.join(webapp.JOBS_ROOT, ".incoming-" + uuid.uuid4().hex)
        os.makedirs(stale)
        with open(os.path.join(stale, "tasks.csv"), "w", encoding="utf-8") as fh:
            fh.write("任务号,层摘要\nT,d\n")
        je.save_state(stale, {"phase": "queued", "total_pairs": 0,
                              "buckets": {}, "skipped_rows": {}})
        # 扫描既不把它当作业执行，也顺手清掉半成品
        webapp._recover_on_startup()
        self.assertFalse(os.path.exists(stale))
        self.assertEqual(webapp._threads, {})
        page = self.client.get("/").data.decode("utf-8")
        self.assertNotIn("incoming", page)
        self.assertIn("暂无作业", page)

    def test_write_failure_returns_error_and_leaves_no_partial_job(self):
        def boom(storage, path):
            raise OSError("模拟磁盘写满")

        old = webapp._save_upload_durably
        webapp._save_upload_durably = boom
        try:
            resp = self._upload([("T", "d")], [("M", "d")])
        finally:
            webapp._save_upload_durably = old
        self.assertEqual(resp.status_code, 500)
        self.assertIn("未接单", resp.data.decode("utf-8"))
        # 没有任何作业目录、暂存目录或队列条目
        self.assertEqual(os.listdir(webapp.JOBS_ROOT), [])
        self.assertFalse(webapp._tracked)
        self.assertFalse(webapp._queued_new)
        self.assertFalse(webapp._threads)

    def test_state_write_failure_leaves_no_partial_job(self):
        def boom(job_dir, state):
            raise OSError("模拟状态落盘失败")

        old = webapp.save_state
        webapp.save_state = boom
        try:
            resp = self._upload([("T", "d")], [("M", "d")])
        finally:
            webapp.save_state = old
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(os.listdir(webapp.JOBS_ROOT), [])
        self.assertFalse(webapp._tracked)
        self.assertFalse(webapp._threads)

    def test_upload_accepted_only_after_state_on_disk(self):
        observed = {}
        old = webapp.save_state

        def checking_save(job_dir, state):
            old(job_dir, state)
            # 接单前，正式作业目录与可恢复状态必须已经存在
            observed["phase"] = state.get("phase")
            observed["state_exists"] = os.path.exists(
                os.path.join(job_dir, "state.json"))

        webapp.save_state = checking_save
        webapp.MAX_CONCURRENT_JOBS = 1
        webapp.MAX_RECOVERED_RUNNING = 1
        fake, events, calls = self._blocking_run_join()
        old_run = webapp.run_join
        webapp.run_join = fake
        try:
            job_id = self._job_id(self._upload([("T", "d")], [("M", "d")]))
            self.assertEqual(observed, {"phase": "queued", "state_exists": True})
            _wait_until(lambda: job_id in events)
            # 开跑时引擎把 queued 原子推进为 partitioning/后续阶段
            jdir = os.path.join(webapp.JOBS_ROOT, job_id)
            self.assertIn(je.load_state(jdir)["phase"],
                          ("partitioning", "joining"))
            events[job_id].set()
            _wait_until(lambda: not webapp._tracked)
        finally:
            webapp.run_join = old_run
            webapp.save_state = old


if __name__ == "__main__":
    unittest.main(verbosity=2)
