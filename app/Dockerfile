FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    JOBS_DIR=/data/jobs

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN mkdir -p /data/jobs

EXPOSE 8000

# 单进程 + 多线程：后台配对线程与状态文件同进程，避免多 worker 抢跑
CMD ["python", "app.py"]
