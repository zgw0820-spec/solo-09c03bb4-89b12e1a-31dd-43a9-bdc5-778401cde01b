"""Web 层自测：上传、状态轮询、下载门禁、进程重启后的恢复。

运行（需 Flask）：
  PYTHONPATH=.pylibs:app python3 -I -m unittest discover -s tests -t . -v
"""

from __future__ import annotations

import csv
import io
import os
import shutil
import sys
import tempfile
import time
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (os.path.join(REPO_ROOT, "app"),
           os.path.join(REPO_ROOT, ".pylibs")):
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

import app as webapp  # noqa: E402


def _csv_bytes(header: list, rows: list) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


class WebFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        webapp.JOBS_ROOT = os.path.join(self.tmp, "jobs")
        os.makedirs(webapp.JOBS_ROOT, exist_ok=True)
        webapp._recovered = False
        webapp._threads.clear()
        webapp.app.config["TESTING"] = True
        self.client = webapp.app.test_client()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

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

    def _wait_done(self, job_id: str, timeout: float = 30.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = self.client.get(f"/jobs/{job_id}/status")
            body = r.get_json()
            if body["phase"] in ("done", "failed"):
                return body
            time.sleep(0.1)
        self.fail("作业未在限定时间内结束")

    def test_upload_status_download_gating(self):
        tasks = [("T1", "d1"), ("T2", "d1"), ("T3", "d2")]
        machines = [("M1", "d1"), ("M2", "d1"), ("M3", "d2")]
        resp = self._upload(tasks, machines)
        self.assertEqual(resp.status_code, 302)
        job_id = resp.headers["Location"].rstrip("/").split("/")[-1]

        # 完成前下载应被拒绝（409）
        early = self.client.get(f"/jobs/{job_id}/download")
        self.assertEqual(early.status_code, 409)

        body = self._wait_done(job_id)
        self.assertEqual(body["phase"], "done")
        # d1: 2x2=4, d2: 1x1=1
        self.assertEqual(body["confirmed_pairs"], 5)
        self.assertTrue(body["download_ready"])

        # 完成后下载开放，列名符合要求
        dl = self.client.get(f"/jobs/{job_id}/download")
        self.assertEqual(dl.status_code, 200)
        text = dl.data.decode("utf-8")
        lines = list(csv.reader(io.StringIO(text)))
        self.assertEqual(lines[0], ["任务号", "来源构建机", "层摘要"])
        self.assertEqual(len(lines), 6)  # 表头 + 5 对
        pairs = [tuple(r) for r in lines[1:]]
        self.assertEqual(len(set(pairs)), 5)
        self.assertIn(("T2", "M2", "d1"), pairs)

    def test_bad_header_rejected(self):
        data = {
            "tasks_csv": (io.BytesIO(_csv_bytes(["wrong", "headers"],
                                                [("a", "b")])), "t.csv"),
            "machines_csv": (io.BytesIO(_csv_bytes(
                ["来源构建机", "层摘要"], [("M1", "d1")])), "m.csv"),
        }
        resp = self.client.post(
            "/upload", data=data, content_type="multipart/form-data")
        self.assertEqual(resp.status_code, 400)

    def test_recovery_after_process_restart(self):
        """模拟重启：作业进行中直接构造一个 joining 中断态，再调用启动恢复。"""
        tasks = [(f"T{i}", f"d{i % 11}") for i in range(300)]
        machines = [(f"M{i}", f"d{i % 11}") for i in range(300)]
        resp = self._upload(tasks, machines)
        job_id = resp.headers["Location"].rstrip("/").split("/")[-1]
        self._wait_done(job_id)

        # 人为回退状态到 joining，模拟"重启前写盘中断"
        import json
        jdir = os.path.join(webapp.JOBS_ROOT, job_id)
        state_path = os.path.join(jdir, "state.json")
        with open(state_path, encoding="utf-8") as fh:
            state = json.load(fh)
        confirmed_before = sum(
            1 for n in os.listdir(os.path.join(jdir, "results"))
            if n.endswith(".csv"))
        state["phase"] = "joining"
        with open(state_path, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, sort_keys=True)

        # 重新执行启动恢复逻辑：应续跑而非重算已确认批次
        webapp._recover_on_startup()
        body = self._wait_done(job_id)
        self.assertEqual(body["phase"], "done")
        # 已确认批次文件仍在，未被删除重建
        self.assertGreaterEqual(
            len([n for n in os.listdir(os.path.join(jdir, "results"))
                 if n.endswith(".csv")]), confirmed_before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
