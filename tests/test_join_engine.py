"""引擎自测：正确性、热点摘要内存上限、中断续跑（进程内 + 子进程）。

直接用标准库运行：
  python -m unittest -v tests.test_join_engine
（不依赖 Flask，CI 中无需装第三方包即可验证核心算法。）
"""

from __future__ import annotations

import csv
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "app"))

import join_engine as je  # noqa: E402


def _write_csv(path: str, header: list, rows: list) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def _make_job(root: str, task_rows: list, machine_rows: list) -> str:
    jdir = os.path.join(root, "job")
    os.makedirs(jdir, exist_ok=True)
    _write_csv(os.path.join(jdir, "tasks.csv"),
               ["任务号", "层摘要"], task_rows)
    _write_csv(os.path.join(jdir, "machines.csv"),
               ["来源构建机", "层摘要"], machine_rows)
    return jdir


def _read_pairs(path: str) -> list:
    with open(path, "r", encoding="utf-8", newline="") as fh:
        r = csv.reader(fh)
        header = next(r)
        return [tuple(row) for row in r], header


class CorrectnessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_all_pairs_listed_once(self):
        # 摘要 d1: 2 任务 x 3 机器 = 6 对；d2: 2 x 2 = 4 对；d3 无匹配
        tasks = [("T1", "d1"), ("T2", "d1"), ("T3", "d2"),
                 ("T4", "d2"), ("T5", "d3")]
        machines = [("M1", "d1"), ("M2", "d1"), ("M3", "d1"),
                    ("M4", "d2"), ("M5", "d2"), ("M6", "d9")]
        jdir = _make_job(self.tmp, tasks, machines)
        state = je.run_join(jdir)

        self.assertEqual(state["phase"], "done")
        self.assertEqual(state["total_pairs"], 10)
        pairs, header = _read_pairs(os.path.join(jdir, je.FINAL_RESULT_FILE))
        self.assertEqual(header, ["任务号", "来源构建机", "层摘要"])
        self.assertEqual(len(pairs), 10)
        # 每对唯一
        self.assertEqual(len(set(pairs)), 10)
        d1 = {p for p in pairs if p[2] == "d1"}
        self.assertEqual(len(d1), 6)
        d2 = {p for p in pairs if p[2] == "d2"}
        self.assertEqual(len(d2), 4)
        # 输出列内容正确
        self.assertIn(("T1", "M2", "d1"), pairs)
        self.assertIn(("T4", "M5", "d2"), pairs)

    def test_empty_digest_rows_skipped(self):
        tasks = [("T1", "d1"), ("T2", "")]
        machines = [("M1", "d1"), ("M2", "")]
        jdir = _make_job(self.tmp, tasks, machines)
        state = je.run_join(jdir)
        self.assertEqual(state["total_pairs"], 1)
        self.assertEqual(state["skipped_rows"]["tasks"], 1)
        self.assertEqual(state["skipped_rows"]["machines"], 1)

    def test_no_match(self):
        tasks = [("T1", "aaa")]
        machines = [("M1", "bbb")]
        jdir = _make_job(self.tmp, tasks, machines)
        state = je.run_join(jdir)
        self.assertEqual(state["total_pairs"], 0)
        # 无配对也应产出只含表头的最终文件，下载可开放
        self.assertTrue(os.path.exists(os.path.join(jdir, je.FINAL_RESULT_FILE)))


class HotDigestMemoryTests(unittest.TestCase):
    """热点摘要：同一摘要海量匹配，验证内存驻留严格不超过 256/256。"""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_hot_digest_memory_bounded(self):
        n_tasks = 600
        n_machines = 600
        # 全部同一摘要 -> 36 万个行对，远超内存；另加少量冷数据
        tasks = [(f"T{i}", "hot") for i in range(n_tasks)]
        tasks += [(f"C{i}", f"cold{i % 50}") for i in range(200)]
        machines = [(f"M{i}", "hot") for i in range(n_machines)]
        machines += [(f"H{i}", f"cold{i % 50}") for i in range(200)]
        jdir = _make_job(self.tmp, tasks, machines)

        # 注入守卫跑完整流程：任何阶段超 256/256 都会直接抛 MemoryLimitExceeded
        full_guard = je.BoundedMemoryGuard()
        state = je.run_join(jdir, guard=full_guard)
        self.assertLessEqual(full_guard.peak_records, je.MAX_RECORDS_IN_MEMORY)
        self.assertLessEqual(full_guard.peak_results, je.MAX_RESULTS_IN_MEMORY)
        # 热点路径确实把结果缓冲用满到 256 后刷盘
        self.assertEqual(full_guard.peak_results, je.MAX_RESULTS_IN_MEMORY)
        expected = n_tasks * n_machines + sum(
            1 for i in range(200) for k in range(200)
            if f"cold{i % 50}" == f"cold{k % 50}")
        self.assertEqual(state["total_pairs"], expected)
        self.assertEqual(expected, 360_800)

        # 重新跑一次并挂守卫探针，直接断言峰值（引擎内部已用守卫强制）
        guard = je.BoundedMemoryGuard()
        # 复用已分桶文件做一次独立的块级检查
        part = os.path.join(jdir, je.PARTITION_DIR)
        hot_bucket = je._bucket_index("hot")
        t_block = je._read_block(
            os.path.join(part, f"tasks_{hot_bucket:03d}.part"), 0,
            je.TASK_SIDE, guard)
        m_block = je._read_block(
            os.path.join(part, f"machines_{hot_bucket:03d}.part"), 0,
            je.MACHINE_SIDE, guard)
        self.assertLessEqual(guard.peak_records, je.MAX_RECORDS_IN_MEMORY)
        self.assertEqual(len(t_block) + len(m_block), 2 * je.BLOCK_SIZE)

        # 模拟结果累计：批大小 256，达上限必须先刷盘
        batch = []
        peak_results = 0
        for t_rec in t_block:
            for m_rec in m_block:
                if t_rec.key == m_rec.key:
                    guard.acquire_result()
                    batch.append((t_rec, m_rec))
                    peak_results = max(peak_results, guard.results)
                    if len(batch) >= je.RESULT_BATCH_SIZE:
                        guard.release_result(len(batch))
                        batch = []
        if batch:
            guard.release_result(len(batch))
        self.assertLessEqual(peak_results, je.MAX_RESULTS_IN_MEMORY)
        self.assertEqual(guard.peak_results, je.MAX_RESULTS_IN_MEMORY)


class CrashResumeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _data(self):
        # 足够多的数据以产生多个桶、多个块、多个结果批
        tasks = [(f"T{i}", f"d{i % 37}") for i in range(800)]
        machines = [(f"M{i}", f"d{i % 37}") for i in range(800)]
        return tasks, machines

    def test_in_process_crash_and_resume(self):
        tasks, machines = self._data()
        jdir = _make_job(self.tmp, tasks, machines)

        # 在 join 阶段注入故障（分桶约需 1600 个检查点，取靠后的点位，
        # 确保已有部分单元确认落盘）
        with self.assertRaises(RuntimeError):
            je.run_join(jdir, crash_hook=_CountingCrash(1610))

        mid_pairs = je.count_pairs_on_disk(jdir)
        self.assertGreater(mid_pairs, 0)
        state_mid = je.load_state(jdir)
        self.assertIn(state_mid["phase"], ("partitioning", "joining"))

        # 续跑：已确认单元不重算，最终结果与一次跑完一致
        final_state = je.run_join(jdir)
        self.assertEqual(final_state["phase"], "done")

        fresh = tempfile.mkdtemp()
        try:
            ref_dir = _make_job(fresh, tasks, machines)
            ref_state = je.run_join(ref_dir)
            self.assertEqual(final_state["total_pairs"], ref_state["total_pairs"])
            ref_pairs, _ = _read_pairs(os.path.join(ref_dir, je.FINAL_RESULT_FILE))
        finally:
            shutil.rmtree(fresh, ignore_errors=True)
        got_pairs, _ = _read_pairs(os.path.join(jdir, je.FINAL_RESULT_FILE))
        self.assertEqual(sorted(got_pairs), sorted(ref_pairs))
        # 无重复对
        self.assertEqual(len(got_pairs), len(set(got_pairs)))

    def test_subprocess_crash_and_resume(self):
        """模拟真实写盘中断：两个独立进程，中间进程退出，磁盘状态接力。"""
        tasks, machines = self._data()
        jdir = _make_job(self.tmp, tasks, machines)
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(REPO_ROOT, "app")
        cli = os.path.join(REPO_ROOT, "cli", "run.py")

        # 第一次进程：在 join 阶段（分桶约 1600 检查点之后）被故障注入打断
        p1 = subprocess.run(
            [sys.executable, "-I", cli, jdir, "--crash-after", "1610"],
            capture_output=True, text=True, env=env)
        self.assertEqual(p1.returncode, 3, p1.stderr)
        self.assertGreater(je.count_pairs_on_disk(jdir), 0)

        # 第二次进程：无故障注入，从断点续跑至完成
        p2 = subprocess.run(
            [sys.executable, "-I", cli, jdir],
            capture_output=True, text=True, env=env)
        self.assertEqual(p2.returncode, 0, p2.stderr)

        # 第三次再跑：全部单元已确认，应幂等且不产生重复
        p3 = subprocess.run(
            [sys.executable, "-I", cli, jdir],
            capture_output=True, text=True, env=env)
        self.assertEqual(p3.returncode, 0, p3.stderr)

        pairs, header = _read_pairs(os.path.join(jdir, je.FINAL_RESULT_FILE))
        self.assertEqual(header, ["任务号", "来源构建机", "层摘要"])
        self.assertEqual(len(pairs), len(set(pairs)))
        state = je.load_state(jdir)
        self.assertEqual(state["phase"], "done")

    def test_partition_phase_crash_is_resumable(self):
        tasks, machines = self._data()
        jdir = _make_job(self.tmp, tasks, machines)
        # 第 1 个检查点就在分桶阶段
        with self.assertRaises(RuntimeError):
            je.run_join(jdir, crash_hook=_CountingCrash(1))
        state = je.run_join(jdir)
        self.assertEqual(state["phase"], "done")
        pairs, _ = _read_pairs(os.path.join(jdir, je.FINAL_RESULT_FILE))
        self.assertGreater(len(pairs), 0)
        self.assertEqual(len(pairs), len(set(pairs)))


class _CountingCrash:
    def __init__(self, after: int) -> None:
        self.after = after
        self.calls = 0

    def __call__(self, checkpoint: str) -> None:
        self.calls += 1
        if self.calls >= self.after:
            raise RuntimeError(f"模拟写盘中断 @ {checkpoint}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
