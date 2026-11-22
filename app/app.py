"""Flask 网页：上传两份 CSV，后台执行受限内存配对，完成后开放下载。

调度模型（单实例）：
- 最多 ``MAX_CONCURRENT_JOBS``（默认 2）份作业同时执行。
- 进程重启时扫描出的旧作业（"重启前接单"）最多同时占 ``MAX_RECOVERED_RUNNING``
  （默认 1）个名额；其余空位随时可给新上传的作业。
- 等候中的作业只占内存队列，不创建执行线程；有执行线程结束才补位。
- 启动恢复扫描在独立后台线程进行，首页/作业页/上传都不等扫描结束。
"""

from __future__ import annotations

import os
import shutil
import threading
import uuid
from collections import deque
from typing import Deque, Dict, Optional

from flask import (Flask, Response, abort, redirect, render_template,
                   request, send_file, url_for)

from join_engine import (FINAL_RESULT_FILE, RESUMABLE_PHASES, JoinError,
                         count_pairs_on_disk, load_state, read_csv_header,
                         run_join, save_state)

JOBS_ROOT = os.environ.get("JOBS_DIR", "/data/jobs")
MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 单次上传 50MB 上限

# 同一实例最多同时执行两份；重启前接单的作业最多占其中一份。
MAX_CONCURRENT_JOBS = max(1, int(os.environ.get("MAX_CONCURRENT_JOBS", "2")))
# 至少保留 1 个恢复名额：否则重启前接单的作业会一直排不上队。
MAX_RECOVERED_RUNNING = max(
    1, min(MAX_CONCURRENT_JOBS,
           int(os.environ.get("MAX_RECOVERED_RUNNING", "1"))))

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES

_jobs_lock = threading.RLock()

# 正在执行的作业 -> 执行线程（仅运行中的作业才有线程）
_threads: Dict[str, threading.Thread] = {}
_running_origin: Dict[str, str] = {}   # "new" | "recovered"

# 等候队列：新旧作业分开，便于给重启前作业保留固定名额
_queued_new: Deque[str] = deque()
_queued_recovered: Deque[str] = deque()
_queued_origin: Dict[str, str] = {}

# 本进程已接单（排队或执行中）的作业，用于扫描/上传/页面交错时的去重
_tracked: set = set()
# 正在落盘、尚未接单的上传作业，扫描必须跳过
_uploading: set = set()

_recovered = False
_recovery_started = False
_recovery_thread: Optional[threading.Thread] = None


# --------------------------------------------------------------------------
# 调度器
# --------------------------------------------------------------------------

def _pick_next_locked() -> Optional[tuple]:
    """在持锁状态下选下一个可执行作业。返回 (job_id, origin) 或 None。"""
    recovery_running = sum(
        1 for origin in _running_origin.values() if origin == "recovered")
    # 重启前接单的作业只在有空位、且自身名额未占满时入位。
    if _queued_recovered and recovery_running < MAX_RECOVERED_RUNNING:
        job_id = _queued_recovered.popleft()
        return job_id, "recovered"
    if _queued_new:
        return _queued_new.popleft(), "new"
    return None


def _dispatch_locked() -> None:
    """把队列里的作业补进空闲执行位；等候作业不占线程。"""
    while len(_threads) < MAX_CONCURRENT_JOBS:
        picked = _pick_next_locked()
        if picked is None:
            break
        job_id, origin = picked
        _queued_origin.pop(job_id, None)
        thread = threading.Thread(
            target=_run_job_thread, args=(job_id, origin), daemon=True,
            name=f"pairing-{job_id[:8]}")
        _threads[job_id] = thread
        _running_origin[job_id] = origin
        try:
            thread.start()
        except RuntimeError:
            # 线程起不来（极端资源耗尽）：撤回登记，作业留在队列下次再试
            _threads.pop(job_id, None)
            _running_origin.pop(job_id, None)
            _queued_origin[job_id] = origin
            if origin == "recovered":
                _queued_recovered.appendleft(job_id)
            else:
                _queued_new.appendleft(job_id)
            break


def _enqueue_locked(job_id: str, origin: str) -> bool:
    """持锁入队；同一作业在同一进程内只会有一个执行者。"""
    if job_id in _tracked or job_id in _uploading:
        return False
    _tracked.add(job_id)
    _queued_origin[job_id] = origin
    if origin == "recovered":
        _queued_recovered.append(job_id)
    else:
        _queued_new.append(job_id)
    _dispatch_locked()
    return True


def _enqueue(job_id: str, origin: str) -> bool:
    with _jobs_lock:
        return _enqueue_locked(job_id, origin)


def _run_job_thread(job_id: str, origin: str) -> None:
    jdir = os.path.join(JOBS_ROOT, job_id)
    ran = False
    try:
        state = load_state(jdir)
        if state.get("phase") not in RESUMABLE_PHASES:
            # 扫描与完成交错时可能拿到刚写完的终态，直接放弃，不重跑
            return
        ran = True
        # run_join 会把 queued 原子推进为 partitioning，再按断点继续。
        run_join(jdir)
    except Exception as exc:  # noqa: BLE001 - 后台线程需落盘错误信息
        if ran:
            try:
                state = load_state(jdir)
                state["phase"] = "failed"
                state["error"] = str(exc)
                save_state(jdir, state)
            except Exception:  # noqa: BLE001 - 错误落盘失败时不再掩盖原始异常
                pass
    finally:
        with _jobs_lock:
            _threads.pop(job_id, None)
            _running_origin.pop(job_id, None)
            _queued_origin.pop(job_id, None)
            _tracked.discard(job_id)
            _dispatch_locked()


# --------------------------------------------------------------------------
# 启动恢复（后台扫描）
# --------------------------------------------------------------------------

def _is_job_id(name: str) -> bool:
    """作业目录名允许 uuid hex / 带连字符；点开头的是落盘暂存目录。"""
    return bool(name) and not name.startswith(".") and name.replace("-", "").isalnum()


_STAGING_PREFIX = ".incoming-"


def _cleanup_stale_staging() -> None:
    """清理上次进程崩溃留下的上传暂存目录（尚未确认接单的半成品）。

    正在本进程上传的目录在 ``_uploading`` 中，不删。
    """
    try:
        names = os.listdir(JOBS_ROOT)
    except OSError:
        return
    for name in names:
        if not name.startswith(_STAGING_PREFIX):
            continue
        job_id = name[len(_STAGING_PREFIX):]
        with _jobs_lock:
            if job_id in _uploading:
                continue
        shutil.rmtree(os.path.join(JOBS_ROOT, name), ignore_errors=True)


def _recover_on_startup() -> None:
    """扫描作业目录，把可续跑的作业重新入队。

    这是同步函数，便于测试直接调用；正常启动时由 ``_ensure_recovery_started``
    放到后台线程执行，页面请求无需等待目录扫描。
    """
    os.makedirs(JOBS_ROOT, exist_ok=True)
    _cleanup_stale_staging()
    try:
        names = sorted(os.listdir(JOBS_ROOT))
    except OSError:
        return
    for job_id in names:
        if not _is_job_id(job_id):
            continue
        jdir = os.path.join(JOBS_ROOT, job_id)
        try:
            if not os.path.isdir(jdir) or not os.path.exists(
                    os.path.join(jdir, "state.json")):
                continue
            # 持锁重读状态再入队：与完工线程、上传线程交错时也只入队一次。
            with _jobs_lock:
                if job_id in _tracked or job_id in _uploading:
                    continue
                state = load_state(jdir)
                if state.get("phase") not in RESUMABLE_PHASES:
                    continue
                _enqueue_locked(job_id, "recovered")
        except Exception:  # noqa: BLE001 - 单个坏作业不阻断整次扫描
            continue


def _start_recovery_once() -> None:
    global _recovery_started, _recovery_thread
    with _jobs_lock:
        if _recovered or _recovery_started:
            return
        _recovery_started = True
        _recovery_thread = threading.Thread(
            target=_recovery_worker, daemon=True, name="startup-recovery")
        thread = _recovery_thread
    thread.start()


def _recovery_worker() -> None:
    global _recovered, _recovery_started
    ok = False
    try:
        _recover_on_startup()
        ok = True
    finally:
        with _jobs_lock:
            # 扫描整体失败时不标记完成，后续请求可再次触发重试
            _recovered = ok
            _recovery_started = False


@app.before_request
def _before_request() -> None:
    # 只负责"踢"一下后台扫描，不等它结束。
    _start_recovery_once()


# --------------------------------------------------------------------------
# 作业信息 / 页面
# --------------------------------------------------------------------------

def _job_dir(job_id: str) -> str:
    # job_id 是 uuid，禁止任何路径穿越
    if not job_id.replace("-", "").isalnum():
        abort(404)
    path = os.path.join(JOBS_ROOT, job_id)
    if not os.path.isdir(path):
        abort(404)
    return path


def _job_summary(job_id: str) -> dict:
    jdir = _job_dir(job_id)
    state = load_state(jdir)
    # 页面统计只认重启后仍保留在磁盘上的已确认配对
    state["confirmed_pairs"] = count_pairs_on_disk(jdir)
    state["download_ready"] = os.path.exists(os.path.join(jdir, FINAL_RESULT_FILE))
    state["job_id"] = job_id
    with _jobs_lock:
        if job_id in _threads:
            queue_status = "running"
        elif job_id in _tracked:
            queue_status = "queued"
        else:
            queue_status = None
    state["queue_status"] = queue_status
    state["running"] = queue_status == "running"
    state["queued"] = queue_status == "queued"
    return state


@app.get("/")
def index() -> str:
    jobs = []
    if os.path.isdir(JOBS_ROOT):
        for job_id in sorted(os.listdir(JOBS_ROOT), reverse=True):
            if not _is_job_id(job_id):
                continue
            jdir = os.path.join(JOBS_ROOT, job_id)
            if os.path.isdir(jdir) and os.path.exists(os.path.join(jdir, "state.json")):
                jobs.append(_job_summary(job_id))
    return render_template("index.html", jobs=jobs)


# --------------------------------------------------------------------------
# 上传：落盘成功才接单
# --------------------------------------------------------------------------

def _save_upload_durably(storage, path: str) -> None:
    """流式写文件并 fsync；调用方负责失败时清理暂存目录。"""
    stream = storage.stream
    try:
        stream.seek(0)
    except (AttributeError, OSError):
        pass
    with open(path, "wb") as out:
        shutil.copyfileobj(stream, out, length=1024 * 1024)
        out.flush()
        os.fsync(out.fileno())


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _initial_state() -> dict:
    return {
        "phase": "queued",       # 已接单、等候执行
        "total_pairs": 0,
        "buckets": {},
        "skipped_rows": {"tasks": 0, "machines": 0},
    }


@app.post("/upload")
def upload() -> Response:
    tasks_file = request.files.get("tasks_csv")
    machines_file = request.files.get("machines_csv")
    if not tasks_file or not machines_file:
        return render_template("error.html",
                               message="需要同时上传任务 CSV 和构建机 CSV 两个文件。"), 400

    job_id = uuid.uuid4().hex
    jdir = os.path.join(JOBS_ROOT, job_id)
    # 暂存目录以点开头，扫描器会忽略；完整落盘后再原子改名为正式作业目录。
    staging = os.path.join(JOBS_ROOT, f".incoming-{job_id}")
    published = False
    try:
        os.makedirs(JOBS_ROOT, exist_ok=True)
        with _jobs_lock:
            _uploading.add(job_id)
        os.makedirs(staging)
        tasks_path = os.path.join(staging, "tasks.csv")
        machines_path = os.path.join(staging, "machines.csv")
        _save_upload_durably(tasks_file, tasks_path)
        _save_upload_durably(machines_file, machines_path)
        from join_engine import MACHINE_SIDE, TASK_SIDE, _resolve_columns
        try:
            _resolve_columns(read_csv_header(tasks_path), TASK_SIDE)
            _resolve_columns(read_csv_header(machines_path), MACHINE_SIDE)
        except JoinError as exc:
            shutil.rmtree(staging, ignore_errors=True)
            with _jobs_lock:
                _uploading.discard(job_id)
            return render_template("error.html", message=str(exc)), 400

        # 可恢复状态落盘（含目录 fsync）之后，才把作业目录发布出去。
        save_state(staging, _initial_state())
        _fsync_dir(staging)
        os.replace(staging, jdir)
        published = True
        _fsync_dir(JOBS_ROOT)
    except Exception as exc:  # noqa: BLE001 - 任何落盘失败都不接单
        shutil.rmtree(staging, ignore_errors=True)
        if published:
            # 已改名但持久化未确认：撤下未接单的作业，避免半成品留在目录里
            shutil.rmtree(jdir, ignore_errors=True)
        with _jobs_lock:
            _uploading.discard(job_id)
        return render_template(
            "error.html",
            message=f"作业落盘失败，未接单：{exc}"), 500

    # 文件与状态都已持久化，此时才确认接单并进入（可能等候的）队列。
    # 撤销 uploading 标记与入队在同一个锁内完成，扫描线程不会把它当成待恢复作业。
    with _jobs_lock:
        _uploading.discard(job_id)
        _enqueue_locked(job_id, "new")
    return redirect(url_for("job_detail", job_id=job_id))


# --------------------------------------------------------------------------
# 作业详情 / 下载 / 状态
# --------------------------------------------------------------------------

@app.get("/jobs/<job_id>")
def job_detail(job_id: str) -> str:
    summary = _job_summary(job_id)
    return render_template("job.html", job=summary)


@app.get("/jobs/<job_id>/download")
def download(job_id: str) -> Response:
    jdir = _job_dir(job_id)
    final_path = os.path.join(jdir, FINAL_RESULT_FILE)
    # 配对全部完成（最终文件生成）后才开放下载
    if not os.path.exists(final_path):
        abort(409, description="配对尚未全部完成，下载暂未开放。")
    return send_file(
        final_path, as_attachment=True, download_name="layer_pairs.csv",
        mimetype="text/csv")


@app.get("/jobs/<job_id>/status")
def status(job_id: str) -> Response:
    from flask import jsonify
    return jsonify(_job_summary(job_id))


if __name__ == "__main__":
    # 启动恢复在后台线程扫描，Web 服务立即可用
    _start_recovery_once()
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
