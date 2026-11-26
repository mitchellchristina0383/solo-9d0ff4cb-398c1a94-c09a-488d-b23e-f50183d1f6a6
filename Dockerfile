FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data
ENV DRILL_DB=/data/drill.db

EXPOSE 5000

# SQLite 本地演练：单 worker + 多线程即可，避免多 worker 并发写歧义
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "4", "--no-control-socket", "--access-logfile", "-", "wsgi:application"]
