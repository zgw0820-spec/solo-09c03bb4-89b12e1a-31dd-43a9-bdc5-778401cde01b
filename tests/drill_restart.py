"""端到端演练：真实 Flask 进程 + 真实引擎，模拟"积压旧作业时上传短作业、
排队时重启"。

不依赖 Docker：本机无容器运行时时，用同一份 app 代码以独立进程运行，
数据目录语义与 Docker Compose 里挂载的卷一致（JOBS_DIR）。

用法：
  python3 -I tests/drill_restart.py                    # 完整演练
  python3 -I tests/drill_restart.py --keep             # 保留数据目录
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_DIR = os.path.join(REPO_ROOT, "app")
PYLIBS = os.path.join(REPO_ROOT, ".pylibs")
PORT = 8010
BASE = f"http://127.0.0.1:{PORT}"


def _env(jobs_dir: str) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([PYLIBS, APP_DIR])
    env["JOBS_DIR"] = jobs_dir
    env["PORT"] = str(PORT)
    env["PYTHONUNBUFFERED"] = "1"
    return env


def start_server(jobs_dir: str) -> subprocess.Popen:
    # 不用 -I：需要 PYTHONPATH 提供本地 .pylibs 里的 Flask（与 README 本地开发一致）
    proc = subprocess.Popen(
        [sys.executable, os.path.join(APP_DIR, "app.py")],
        env=_env(jobs_dir), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True)
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"服务启动失败：{proc.stdout.read()}")
        try:
            urllib.request.urlopen(BASE + "/", timeout=1).read()
            return proc
        except (urllib.error.URLError, ConnectionError, urllib.error.HTTPError):
            time.sleep(0.05)
    raise RuntimeError("服务未在 30 秒内就绪")


def stop_server(proc: subprocess.Popen, sig=signal.SIGKILL) -> None:
    if proc.poll() is None:
        proc.send_signal(sig)
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def upload(jobs_dir: str, name: str, tasks, machines) -> str:
    """用 curl 走真实 HTTP multipart 上传，返回 job_id。"""
    tpath = os.path.join(jobs_dir, f".upload-{name}-tasks.csv")
    mpath = os.path.join(jobs_dir, f".upload-{name}-machines.csv")
    for path, header, rows in (
            (tpath, ["任务号", "层摘要"], tasks),
            (mpath, ["来源构建机", "层摘要"], machines)):
        with open(path, "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(header)
            w.writerows(rows)
    out = subprocess.run(
        ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code} %{redirect_url}",
         "-F", f"tasks_csv=@{tpath};type=text/csv",
         "-F", f"machines_csv=@{mpath};type=text/csv",
         BASE + "/upload"],
        capture_output=True, text=True, check=True).stdout.strip()
    os.unlink(tpath)
    os.unlink(mpath)
    code, location = out.split(" ", 1)
    if code != "302":
        raise RuntimeError(f"上传失败：{out}")
    return location.rstrip("/").split("/")[-1]


def _open(path: str, timeout: float = 30.0):
    """带重试的 GET：引擎满负荷时请求线程可能被 GIL 拖延，重试避免误判。"""
    deadline = time.time() + timeout
    while True:
        try:
            return urllib.request.urlopen(BASE + path, timeout=10)
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if time.time() >= deadline:
                raise
            time.sleep(0.2)


def get_json(path: str) -> dict:
    with _open(path) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_text(path: str) -> str:
    with _open(path) as resp:
        return resp.read().decode("utf-8")


def wait_for(predicate, timeout: float, message: str):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(message)


def make_backlog(jobs_dir: str, count: int, n: int):
    """构造"重启前接单"的积压作业：直接落盘 tasks/machines + state=joining。

    重启后由恢复扫描发现并续跑，天然占据恢复名额。
    """
    backlog = []
    for _ in range(count):
        job_id = uuid.uuid4().hex
        jdir = os.path.join(jobs_dir, job_id)
        os.makedirs(jdir)
        with open(os.path.join(jdir, "tasks.csv"), "w", encoding="utf-8",
                  newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["任务号", "层摘要"])
            w.writerows((f"OLD{i}", "hot") for i in range(n))
        with open(os.path.join(jdir, "machines.csv"), "w", encoding="utf-8",
                  newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["来源构建机", "层摘要"])
            w.writerows((f"OLDM{i}", "hot") for i in range(n))
        with open(os.path.join(jdir, "state.json"), "w", encoding="utf-8") as fh:
            json.dump({"phase": "partitioning", "total_pairs": 0, "buckets": {},
                       "skipped_rows": {"tasks": 0, "machines": 0}},
                      fh, ensure_ascii=False)
        backlog.append(job_id)
    return backlog


def _expected_pairs(n: int, modulus: int) -> int:
    """n 行任务 × n 行构建机，摘要 d{i % modulus}：各摘要行数平方和。"""
    counts = [0] * modulus
    for i in range(n):
        counts[i % modulus] += 1
    return sum(c * c for c in counts)


def wait_status(job_id: str, timeout: float, want=("done", "failed")) -> dict:
    def check():
        body = get_json(f"/jobs/{job_id}/status")
        if body["phase"] in want:
            return body
        return None
    return wait_for(check, timeout, f"作业 {job_id[:8]} 未在 {timeout}s 内进入 {want}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/tmp/layer-matcher-drill")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--backlog", type=int, default=2)
    args = parser.parse_args()

    jobs_dir = os.path.join(args.root, "jobs")
    if os.path.isdir(args.root):
        shutil.rmtree(args.root)
    os.makedirs(jobs_dir)
    report = []
    failures = []

    def check(label: str, ok: bool, detail: str = "") -> None:
        report.append((label, ("通过" if ok else "失败")
                       + (f"（{detail}）" if detail else "")))
        if not ok:
            failures.append(label)

    server = None
    try:
        # ---- 阶段 1：有积压旧作业时冷启动 ----
        backlog = make_backlog(jobs_dir, args.backlog, n=600)
        t0 = time.time()
        server = start_server(jobs_dir)
        boot_secs = time.time() - t0
        report.append(("冷启动就绪耗时", f"{boot_secs:.2f}s"))

        index_html = get_text("/")
        check("启动后立即可访问首页（不等目录扫描）", "上传两份 CSV" in index_html)

        # 旧作业占用恢复名额：最多 1 份运行、其余等候
        wait_for(lambda: any(
            get_json(f"/jobs/{j}/status")["queue_status"] == "running"
            for j in backlog), 20, "积压作业未开始执行")
        running_old = [j for j in backlog
                       if get_json(f"/jobs/{j}/status")["queue_status"] == "running"]
        queued_old = [j for j in backlog
                      if get_json(f"/jobs/{j}/status")["queue_status"] == "queued"]
        check("重启前作业最多占一个执行名额", len(running_old) <= 1,
              f"运行 {len(running_old)}，等候 {len(queued_old)}")
        detail_page = get_text("/")
        check("页面区分运行与等候",
              "运行中" in detail_page and (
                  len(queued_old) == 0 or "等候执行" in detail_page))

        # ---- 阶段 2：恢复仍跑积压时，上传短作业 ----
        t_up = time.time()
        short = upload(jobs_dir, "short",
                       [("NEW1", "d1"), ("NEW2", "d1")],
                       [("M1", "d1"), ("M2", "d1"), ("M3", "d1")])
        up_secs = time.time() - t_up
        body = wait_status(short, 20)
        check("扫描/积压期间仍可上传并接单", body["phase"] == "done",
              f"上传到完成 {up_secs:.2f}s")
        check("短作业结果正确", body["confirmed_pairs"] == 6,
              f"配对 {body['confirmed_pairs']}（期望 6）")
        with _open(f"/jobs/{short}/download") as resp:
            rows = list(csv.reader(resp.read().decode("utf-8").splitlines()))
        check("下载清单表头与行数",
              rows[0] == ["任务号", "来源构建机", "层摘要"] and len(rows) == 7,
              f"{len(rows)} 行")

        # ---- 阶段 3：排队场景，准备重启 ----
        big_tasks = [(f"B{i}", f"d{i % 7}") for i in range(1200)]
        big_machines = [(f"BM{i}", f"d{i % 7}") for i in range(1200)]
        first = upload(jobs_dir, "b1", big_tasks, big_machines)
        wait_for(lambda: get_json(
            f"/jobs/{first}/status")["queue_status"] == "running",
            20, "b1 未开始执行")
        second = upload(jobs_dir, "b2", big_tasks, big_machines)
        third = upload(jobs_dir, "b3", big_tasks[:400], big_machines[:400])
        wait_for(lambda: get_json(
            f"/jobs/{second}/status")["queue_status"] == "queued",
            20, "b2 未进入等候")
        with open(os.path.join(jobs_dir, second, "state.json"),
                  encoding="utf-8") as fh:
            queued_phase = json.load(fh)["phase"]
        check("等候作业不占执行线程（状态=queued 落盘）", queued_phase == "queued",
              queued_phase)

        # ---- 阶段 4：硬杀进程（模拟崩溃），重启 ----
        stop_server(server)   # SIGKILL
        server = None
        t0 = time.time()
        server = start_server(jobs_dir)
        report.append(("重启就绪耗时", f"{time.time() - t0:.2f}s"))
        check("重启后首页立即可用", "上传两份 CSV" in get_text("/"))

        # 已接单未开跑：重启后必须执行完
        q2 = wait_status(second, 300)
        check("已接单未开跑作业重启后入队完成", q2["phase"] == "done",
              f"配对 {q2['confirmed_pairs']}")
        q3 = wait_status(third, 300)
        check("第二个排队作业重启后完成", q3["phase"] == "done",
              f"配对 {q3['confirmed_pairs']}")

        # 已开跑：从断点续跑，结果不重不漏
        st1 = wait_status(first, 300)
        expected_pairs = _expected_pairs(len(big_tasks), 7)
        check("已开跑作业重启后续跑至完成", st1["phase"] == "done",
              f"配对 {st1['confirmed_pairs']}（期望 {expected_pairs}）")
        check("续跑结果不重不漏", st1["confirmed_pairs"] == expected_pairs,
              f"{st1['confirmed_pairs']}")
        with _open(f"/jobs/{first}/download") as resp:
            rows = list(csv.reader(resp.read().decode("utf-8").splitlines()))
        pairs = {tuple(r) for r in rows[1:]}
        check("下载无重复行", len(pairs) == len(rows) - 1 == expected_pairs,
              f"{len(rows) - 1} 行，唯一 {len(pairs)}")

        # 幂等：已完成的作业不会被再次执行
        time.sleep(2)
        check("完成态保持（不被恢复扫描重跑）",
              get_json(f"/jobs/{first}/status")["phase"] == "done")

        # 积压旧作业最终也完成
        for job_id in backlog:
            st = wait_status(job_id, 300)
            check(f"积压作业 {job_id[:8]} 最终完成",
                  st["phase"] == "done", f"配对 {st['confirmed_pairs']}")
    finally:
        if server is not None:
            stop_server(server, signal.SIGTERM)
        for label, detail in report:
            print(f"[{detail}] {label}")
        if failures:
            print(f"\n失败项：{failures}")
        else:
            print("\n全部演练项通过")
        if not args.keep:
            shutil.rmtree(args.root, ignore_errors=True)
    return 1 if failures else 0

if __name__ == "__main__":
    raise SystemExit(main())
