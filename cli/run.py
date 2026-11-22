"""命令行入口：独立执行或断点续跑一个作业目录。

用法：
  python -m cli.run JOB_DIR [--crash-after N]

JOB_DIR 下需已有 tasks.csv 与 machines.csv（与网页上传落盘结构一致）。
--crash-after N：故障注入，执行到第 N 个检查点后抛异常，用于自测中断续跑。
"""

from __future__ import annotations

import argparse
import os
import sys

# 允许从仓库根目录以 python -m cli.run 或直接 python cli/run.py 运行；
# 同时兼容 python -I（隔离模式会忽略 PYTHONPATH，需显式加入 app 目录）
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (_REPO_ROOT, os.path.join(_REPO_ROOT, "app")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from join_engine import (FINAL_RESULT_FILE, JoinError,  # noqa: E402
                         count_pairs_on_disk, run_join)


class CrashAfter:
    def __init__(self, after: int) -> None:
        self.after = after
        self.calls = 0

    def __call__(self, checkpoint: str) -> None:
        self.calls += 1
        if self.calls >= self.after:
            raise RuntimeError(f"模拟写盘中断 @ {checkpoint}")


def main() -> int:
    parser = argparse.ArgumentParser(description="受限内存镜像层配对（可续跑）")
    parser.add_argument("job_dir", help="作业目录，含 tasks.csv / machines.csv")
    parser.add_argument("--crash-after", type=int, default=0,
                        help="故障注入：第 N 个检查点后中断（0 表示不注入）")
    args = parser.parse_args()

    job_dir = os.path.abspath(args.job_dir)
    hook = CrashAfter(args.crash_after) if args.crash_after > 0 else None
    try:
        state = run_join(job_dir, crash_hook=hook)
    except JoinError as exc:
        print(f"输入错误：{exc}", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"已按计划中断：{exc}", file=sys.stderr)
        print(f"当前已确认配对（磁盘）：{count_pairs_on_disk(job_dir)}")
        return 3

    print(f"阶段：{state['phase']}")
    print(f"配对总数：{state['total_pairs']}")
    print(f"结果文件：{os.path.join(job_dir, FINAL_RESULT_FILE)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
