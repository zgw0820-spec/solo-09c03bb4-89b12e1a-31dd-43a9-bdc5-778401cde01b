"""Flask 网页：上传两份 CSV，后台执行受限内存配对，完成后开放下载。"""

from __future__ import annotations

import json
import os
import threading
import uuid
from typing import Dict

from flask import (Flask, Response, abort, redirect, render_template,
                   request, send_file, url_for)

from join_engine import (FINAL_RESULT_FILE, STATE_FILE, JoinError,
                         count_pairs_on_disk, load_state,
                         read_csv_header, run_join)

JOBS_ROOT = os.environ.get("JOBS_DIR", "/data/jobs")
MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 单次上传 50MB 上限

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES

_jobs_lock = threading.Lock()
_threads: Dict[str, threading.Thread] = {}
_recovered = False


def _ensure_recovered() -> None:
    """惰性启动恢复：首次请求（或测试显式调用）时扫描磁盘，把中断作业拉起。"""
    global _recovered
    with _jobs_lock:
        if _recovered:
            return
        _recovered = True
    _recover_on_startup()


@app.before_request
def _before_request() -> None:
    _ensure_recovered()


def _job_dir(job_id: str) -> str:
    # job_id 是 uuid4，禁止任何路径穿越
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
        state["running"] = job_id in _threads and _threads[job_id].is_alive()
    return state


def _run_job_thread(job_id: str) -> None:
    jdir = os.path.join(JOBS_ROOT, job_id)
    try:
        run_join(jdir)
    except Exception as exc:  # noqa: BLE001 - 后台线程需落盘错误信息
        state = load_state(jdir)
        state["phase"] = "failed"
        state["error"] = str(exc)
        tmp = os.path.join(jdir, ".state-error.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, sort_keys=True)
        os.replace(tmp, os.path.join(jdir, STATE_FILE))
    finally:
        with _jobs_lock:
            _threads.pop(job_id, None)


def _spawn(job_id: str) -> None:
    t = threading.Thread(target=_run_job_thread, args=(job_id,), daemon=True)
    with _jobs_lock:
        _threads[job_id] = t
    t.start()


def _recover_on_startup() -> None:
    """进程重启后：把中断在 joining/partitioning 的作业重新拉起；
    已确认批次不重算。页面计数始终来自磁盘，不依赖内存状态。"""
    os.makedirs(JOBS_ROOT, exist_ok=True)
    for job_id in os.listdir(JOBS_ROOT):
        jdir = os.path.join(JOBS_ROOT, job_id)
        if not os.path.isdir(jdir) or not os.path.exists(
                os.path.join(jdir, STATE_FILE)):
            continue
        state = load_state(jdir)
        if state.get("phase") in ("partitioning", "joining"):
            _spawn(job_id)


@app.get("/")
def index() -> str:
    jobs = []
    if os.path.isdir(JOBS_ROOT):
        for job_id in sorted(os.listdir(JOBS_ROOT), reverse=True):
            jdir = os.path.join(JOBS_ROOT, job_id)
            if os.path.isdir(jdir) and os.path.exists(os.path.join(jdir, STATE_FILE)):
                jobs.append(_job_summary(job_id))
    return render_template("index.html", jobs=jobs)


@app.post("/upload")
def upload() -> Response:
    tasks_file = request.files.get("tasks_csv")
    machines_file = request.files.get("machines_csv")
    if not tasks_file or not machines_file:
        return render_template("error.html",
                               message="需要同时上传任务 CSV 和构建机 CSV 两个文件。"), 400

    # 先在临时目录完整保存并校验表头，通过后才生成正式作业，避免脏作业
    job_id = uuid.uuid4().hex
    jdir = os.path.join(JOBS_ROOT, job_id)
    os.makedirs(jdir, exist_ok=True)
    tasks_path = os.path.join(jdir, "tasks.csv")
    machines_path = os.path.join(jdir, "machines.csv")
    tasks_file.save(tasks_path)
    machines_file.save(machines_path)
    try:
        from join_engine import MACHINE_SIDE, TASK_SIDE
        from join_engine import _resolve_columns
        _resolve_columns(read_csv_header(tasks_path), TASK_SIDE)
        _resolve_columns(read_csv_header(machines_path), MACHINE_SIDE)
    except JoinError as exc:
        import shutil
        shutil.rmtree(jdir, ignore_errors=True)
        return render_template("error.html", message=str(exc)), 400

    # 初始 state 落盘后再启动线程，保证任何时刻都能从磁盘恢复
    state = load_state(jdir)
    with open(os.path.join(jdir, STATE_FILE), "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, sort_keys=True)
    _spawn(job_id)
    return redirect(url_for("job_detail", job_id=job_id))


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
    # 容器入口：启动即恢复中断作业（测试中由 before_request 惰性触发）
    _ensure_recovered()
    app.run(host="0.0.0.0", port=8000, threaded=True)
