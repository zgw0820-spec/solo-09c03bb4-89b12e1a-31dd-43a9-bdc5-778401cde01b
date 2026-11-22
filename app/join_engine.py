"""受限内存下的镜像层摘要配对引擎。

两表（任务侧 T、构建机侧 M）按"层摘要"做等值连接，输出所有匹配行对，
每对仅输出一次。算法为典型的外部连接（external join），分两个阶段：

1. 分桶（partition）：顺序流式读取两份 CSV，用哈希把每条记录路由到
   P 个磁盘分桶文件。此阶段驻留内存的输入记录不超过 1 条/侧。
2. 分块嵌套循环连接（block nested-loop join）：逐桶处理，每侧每次只
   载入最多 BLOCK 条记录；任务侧分块 × 机器侧分块做笛卡尔匹配。
   热点摘要（同一摘要海量匹配）只会增加磁盘上的结果批文件数量，
   不会增加内存驻留。

内存上限（由 BoundedMemoryGuard 运行时强制并自测）：
- 暂存输入记录上限 MAX_RECORDS_IN_MEMORY = 256
- 暂存结果记录上限 MAX_RESULTS_IN_MEMORY  = 256

断点续跑：工作单元为 (桶, 任务块序号, 机器块序号)。完成一个单元后
原子更新 state.json（记录该桶已完成的块数），已确认的结果批文件
按单元确定性命名；重启后跳过已完成单元，只重算未确认部分。
"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

# ---- 硬性内存上限（题目要求：各 256）----
MAX_RECORDS_IN_MEMORY = 256
MAX_RESULTS_IN_MEMORY = 256

# 每侧每块 128 条：两块合计 256 条输入记录，正好用满且不超上限。
BLOCK_SIZE = 128
# 64 个哈希桶（只在分桶阶段持有文件句柄，不持有记录）。
PARTITION_COUNT = 64
# 结果每 256 条刷一批到磁盘，内存中结果永不超过 256。
RESULT_BATCH_SIZE = 256

TASK_SIDE = "tasks"
MACHINE_SIDE = "machines"

STATE_FILE = "state.json"
RESULT_DIR = "results"
PARTITION_DIR = "partitions"
FINAL_RESULT_FILE = "pairs.csv"

# 输入 CSV 列名（支持常见中英文别名，全部去空白/小写后匹配）
TASK_ID_ALIASES = ("任务号", "任务id", "task", "task_id", "taskid", "job", "job_id")
MACHINE_ALIASES = ("来源构建机", "构建机", "机器", "machine", "machine_id", "host", "builder")
DIGEST_ALIASES = ("层摘要", "摘要", "digest", "layer_digest", "layer", "sha256", "hash")


class JoinError(Exception):
    """配对过程中的可预期错误。"""


@dataclass
class InputRecord:
    """一条输入记录；raw 保留原始行用于输出，key 为连接键（层摘要）。"""

    key: str
    payload: str
    side: str


class MemoryLimitExceeded(RuntimeError):
    """内存守卫触发（属于程序缺陷，测试用来证明上限有效）。"""


class BoundedMemoryGuard:
    """运行时内存驻留计数守卫。

    只统计引擎显式登记的记录/结果对象，用于自测证明任何阶段都不越界；
    实际刷盘机制保证即使没有守卫，内存占用也有同样的上界。
    """

    def __init__(self, record_limit: int = MAX_RECORDS_IN_MEMORY,
                 result_limit: int = MAX_RESULTS_IN_MEMORY) -> None:
        self.record_limit = record_limit
        self.result_limit = result_limit
        self.records = 0
        self.results = 0
        self.peak_records = 0
        self.peak_results = 0

    def acquire_record(self, n: int = 1) -> None:
        self.records += n
        if self.records > self.record_limit:
            raise MemoryLimitExceeded(
                f"输入记录驻留 {self.records} 超过上限 {self.record_limit}")
        self.peak_records = max(self.peak_records, self.records)

    def release_record(self, n: int = 1) -> None:
        self.records -= n
        if self.records < 0:
            raise RuntimeError("记录计数为负，引擎记账有误")

    def acquire_result(self, n: int = 1) -> None:
        self.results += n
        if self.results > self.result_limit:
            raise MemoryLimitExceeded(
                f"结果驻留 {self.results} 超过上限 {self.result_limit}")
        self.peak_results = max(self.peak_results, self.results)

    def release_result(self, n: int = 1) -> None:
        self.results -= n
        if self.results < 0:
            raise RuntimeError("结果计数为负，引擎记账有误")


def _norm_header(name: str) -> str:
    return (name or "").strip().lstrip("﻿").lower()


def _resolve_columns(fieldnames: Optional[List[str]], side: str
                     ) -> Tuple[str, str]:
    """返回 (键列名, 负载列名)。负载列即任务号列或构建机列。"""
    if not fieldnames:
        raise JoinError("CSV 缺少表头")
    normalized = {_norm_header(c): c for c in fieldnames}
    digest_col = next((normalized[a] for a in DIGEST_ALIASES
                       if a in normalized), None)
    if digest_col is None:
        raise JoinError(f"{side} CSV 缺少层摘要列（digest），实际表头：{fieldnames}")
    aliases = TASK_ID_ALIASES if side == TASK_SIDE else MACHINE_ALIASES
    payload_col = next((normalized[a] for a in aliases if a in normalized), None)
    if payload_col is None:
        label = "任务号" if side == TASK_SIDE else "来源构建机"
        raise JoinError(f"{side} CSV 缺少{label}列，实际表头：{fieldnames}")
    return digest_col, payload_col


def read_csv_header(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        try:
            return next(reader)
        except StopIteration:
            raise JoinError(f"空 CSV 文件：{os.path.basename(path)}")


def _bucket_index(key: str) -> int:
    """纯 Python FNV-1a 32 位哈希，跨进程/跨重启稳定（不依赖内置 hash 随机化）。"""
    h = 0x811C9DC5
    for ch in key.encode("utf-8"):
        h ^= ch
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h % PARTITION_COUNT


def _atomic_write_json(path: str, data: dict) -> None:
    directory = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def load_state(job_dir: str) -> dict:
    path = os.path.join(job_dir, STATE_FILE)
    if not os.path.exists(path):
        return {
            "phase": "partitioning",
            "total_pairs": 0,
            "buckets": {},   # bucket_idx -> {"t_done": int, "m_done": int, "t_blocks": int, "m_blocks": int}
            "skipped_rows": {"tasks": 0, "machines": 0},
        }
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def partition_csv(path: str, side: str, job_dir: str,
                  state: dict, guard: BoundedMemoryGuard,
                  crash_hook: Optional[Callable[[str], None]] = None
                  ) -> int:
    """流式分桶。返回该侧写入的记录条数。"""
    part_dir = os.path.join(job_dir, PARTITION_DIR)
    os.makedirs(part_dir, exist_ok=True)
    # 该侧若未确认完成就中断，旧分桶文件可能不完整，先删除再整侧重写
    for name in os.listdir(part_dir):
        if name.startswith(side + "_") and name.endswith(".part"):
            os.unlink(os.path.join(part_dir, name))
    digest_col, payload_col = _resolve_columns(read_csv_header(path), side)

    handles = {}
    writers = {}
    count = 0
    skipped = 0
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                digest = (row.get(digest_col) or "").strip()
                payload = (row.get(payload_col) or "").strip()
                if not digest:
                    # 空摘要无法参与等值连接，跳过并计数
                    skipped += 1
                    continue
                idx = _bucket_index(digest)
                if idx not in handles:
                    bp = os.path.join(part_dir, f"{side}_{idx:03d}.part")
                    handles[idx] = open(bp, "a", encoding="utf-8", newline="")
                    writers[idx] = csv.writer(handles[idx])
                guard.acquire_record()
                try:
                    writers[idx].writerow([digest, payload])
                    count += 1
                finally:
                    guard.release_record()
                if crash_hook is not None:
                    crash_hook(f"partition:{side}:{count}")
    finally:
        for fh in handles.values():
            fh.flush()
            os.fsync(fh.fileno())
            fh.close()

    # 该侧整侧重写，跳过计数直接覆盖而非累加
    state["skipped_rows"][side] = skipped
    return count


def _read_block(part_path: str, block_no: int, side: str,
                guard: BoundedMemoryGuard) -> List[InputRecord]:
    """读取分桶文件的第 block_no 块（每块 BLOCK_SIZE 条），文件不存在视为空。"""
    block: List[InputRecord] = []
    if not os.path.exists(part_path):
        return block
    with open(part_path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        start = block_no * BLOCK_SIZE
        end = start + BLOCK_SIZE
        for i, row in enumerate(reader):
            if i < start:
                continue
            if i >= end:
                break
            if len(row) < 2:
                continue
            guard.acquire_record()
            block.append(InputRecord(key=row[0], payload=row[1], side=side))
    return block


def _count_blocks(part_path: str) -> int:
    if not os.path.exists(part_path):
        return 0
    with open(part_path, "r", encoding="utf-8", newline="") as fh:
        rows = sum(1 for _ in fh)
    return max(1, (rows + BLOCK_SIZE - 1) // BLOCK_SIZE) if rows else 0


def _unit_prefix(bucket: int, tb: int, mb: int) -> str:
    return f"b{bucket:03d}_t{tb:05d}_m{mb:05d}"


def _batch_path(job_dir: str, bucket: int, tb: int, mb: int, part: int = 0) -> str:
    suffix = "" if part == 0 else f".{part}"
    return os.path.join(
        job_dir, RESULT_DIR,
        f"{_unit_prefix(bucket, tb, mb)}{suffix}.csv")


def _cleanup_unit_artifacts(job_dir: str, bucket: int, tb: int, mb: int) -> None:
    """续跑重算单元前，清掉该单元此前的残留（旧批次与半成品临时文件）。"""
    result_dir = os.path.join(job_dir, RESULT_DIR)
    if not os.path.isdir(result_dir):
        return
    prefix = _unit_prefix(bucket, tb, mb)
    for name in os.listdir(result_dir):
        if name == prefix + ".csv" or name.startswith(prefix + "."):
            os.unlink(os.path.join(result_dir, name))


def _flush_batch(batch: List[Tuple[str, str, str]], path: str,
                 guard: BoundedMemoryGuard) -> int:
    """把一个结果批原子写盘。返回写入条数。"""
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".batch-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerows(batch)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    guard.release_result(len(batch))
    return len(batch)


def run_join(job_dir: str,
             crash_hook: Optional[Callable[[str], None]] = None,
             guard: Optional[BoundedMemoryGuard] = None
             ) -> dict:
    """执行（或断点续跑）整个配对任务，返回最终 state。

    crash_hook: 测试用故障注入钩子，在可恢复检查点抛出异常以模拟写盘中断。
    guard: 可注入内存守卫（默认用 256/256 标准上限），便于自测观测峰值。
    """
    if guard is None:
        guard = BoundedMemoryGuard()
    state = load_state(job_dir)
    state.setdefault("skipped_rows", {"tasks": 0, "machines": 0})
    state.setdefault("buckets", {})

    tasks_csv = os.path.join(job_dir, "tasks.csv")
    machines_csv = os.path.join(job_dir, "machines.csv")
    if not os.path.exists(tasks_csv) or not os.path.exists(machines_csv):
        raise JoinError("缺少输入 CSV，无法执行配对")

    # ---- 阶段 1：分桶（已完成的侧在 state 中标记，重启不重做）----
    if state["phase"] == "partitioning":
        done_sides = state.setdefault("partitioned", [])
        for side, path in ((TASK_SIDE, tasks_csv), (MACHINE_SIDE, machines_csv)):
            if side not in done_sides:
                partition_csv(path, side, job_dir, state, guard, crash_hook)
                done_sides.append(side)
                _atomic_write_json(os.path.join(job_dir, STATE_FILE), state)
        state["phase"] = "joining"
        _atomic_write_json(os.path.join(job_dir, STATE_FILE), state)

    part_dir = os.path.join(job_dir, PARTITION_DIR)

    # ---- 阶段 2：逐桶分块嵌套循环连接 ----
    for idx in range(PARTITION_COUNT):
        t_path = os.path.join(part_dir, f"{TASK_SIDE}_{idx:03d}.part")
        m_path = os.path.join(part_dir, f"{MACHINE_SIDE}_{idx:03d}.part")
        t_blocks = _count_blocks(t_path)
        m_blocks = _count_blocks(m_path)
        if t_blocks == 0 or m_blocks == 0:
            # 该桶不可能产生配对，直接标记完成
            state["buckets"][str(idx)] = {
                "t_blocks": t_blocks, "m_blocks": m_blocks,
                "t_done": t_blocks, "m_done": m_blocks}
            _atomic_write_json(os.path.join(job_dir, STATE_FILE), state)
            continue

        bst = state["buckets"].setdefault(str(idx), {
            "t_blocks": t_blocks, "m_blocks": m_blocks,
            "t_done": 0, "m_done": 0})
        bst["t_blocks"], bst["m_blocks"] = t_blocks, m_blocks

        for tb in range(t_blocks):
            # 已确认完成的任务块（其全部机器块都已刷盘）直接跳过，不重算
            if tb < bst["t_done"]:
                continue
            t_block = _read_block(t_path, tb, TASK_SIDE, guard)
            try:
                for mb in range(m_blocks):
                    if tb == bst["t_done"] and mb < bst["m_done"]:
                        continue
                    m_block = _read_block(m_path, mb, MACHINE_SIDE, guard)
                    # 续跑重算：先清理该单元旧产物，避免重复/半成品
                    _cleanup_unit_artifacts(job_dir, idx, tb, mb)
                    batch_part = 0
                    try:
                        batch: List[Tuple[str, str, str]] = []
                        for t_rec in t_block:
                            for m_rec in m_block:
                                if t_rec.key == m_rec.key:
                                    guard.acquire_result()
                                    batch.append(
                                        (t_rec.payload, m_rec.payload, t_rec.key))
                                    if len(batch) >= RESULT_BATCH_SIZE:
                                        _flush_batch(
                                            batch,
                                            _batch_path(job_dir, idx, tb, mb,
                                                        batch_part),
                                            guard)
                                        batch = []
                                        batch_part += 1
                        if batch:
                            _flush_batch(
                                batch,
                                _batch_path(job_dir, idx, tb, mb, batch_part),
                                guard)
                        if crash_hook is not None:
                            crash_hook(f"join:b{idx}:t{tb}:m{mb}")
                    finally:
                        guard.release_record(len(m_block))

                    # 单元确认：推进 m_done；配对数以磁盘已确认产物为准
                    bst["m_done"] = mb + 1
                    state["total_pairs"] = count_pairs_on_disk(job_dir)
                    _atomic_write_json(os.path.join(job_dir, STATE_FILE), state)

                # 该任务块的全部机器块完成，推进 t_done 并复位 m_done 游标
                bst["t_done"] = tb + 1
                bst["m_done"] = 0
                _atomic_write_json(os.path.join(job_dir, STATE_FILE), state)
            finally:
                guard.release_record(len(t_block))

    # ---- 阶段 3：合并最终结果（流式拼接，不整体载入内存）----
    final_path = os.path.join(job_dir, FINAL_RESULT_FILE)
    _build_final_csv(job_dir, final_path)
    state["phase"] = "done"
    state["total_pairs"] = count_pairs_on_disk(job_dir)
    _atomic_write_json(os.path.join(job_dir, STATE_FILE), state)
    return state


def _build_final_csv(job_dir: str, final_path: str) -> None:
    result_dir = os.path.join(job_dir, RESULT_DIR)
    fd, tmp = tempfile.mkstemp(prefix=".final-", suffix=".tmp", dir=job_dir)
    total = 0
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(["任务号", "来源构建机", "层摘要"])
            if os.path.isdir(result_dir):
                for name in sorted(os.listdir(result_dir)):
                    if not name.endswith(".csv"):
                        continue
                    with open(os.path.join(result_dir, name),
                              "r", encoding="utf-8", newline="") as src:
                        for line in src:
                            out.write(line)
                            total += 1
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, final_path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def count_pairs_on_disk(job_dir: str) -> int:
    """只统计已确认落盘的配对——重启后仍能保留的才计数。"""
    result_dir = os.path.join(job_dir, RESULT_DIR)
    if not os.path.isdir(result_dir):
        return 0
    total = 0
    for name in os.listdir(result_dir):
        if not name.endswith(".csv"):
            continue
        with open(os.path.join(result_dir, name), "r", encoding="utf-8") as fh:
            total += sum(1 for _ in fh)
    return total
