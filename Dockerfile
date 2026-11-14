FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY samples ./samples

# /data 供 SQLite 持久化（compose 挂载命名卷，重启后事件与逐端状态不丢）
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /app /data
USER appuser

EXPOSE 8000 8100 8200

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).status == 200 else 1)"

# 默认启动主服务（契约排程 + 事件分发）；接收端在 compose 里覆盖 command：
#   uvicorn app.receiver:app --host 0.0.0.0 --port 8100
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
