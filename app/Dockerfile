FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    JOBS_DIR=/data/jobs \
    MAX_CONCURRENT_JOBS=2 \
    MAX_RECOVERED_RUNNING=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN mkdir -p /data/jobs

EXPOSE 8000

# 单进程 + 多线程：调度器（最多 2 份并发）与状态文件同进程，
# 避免多 worker 各自恢复扫描、重复执行同一作业。
CMD ["python", "app.py"]
