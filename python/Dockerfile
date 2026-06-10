FROM python:3.11-slim

WORKDIR /app
COPY python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY python/ /app/python/
COPY crates/common/src/ /app/crates/common/src/
COPY QQQ_0DTE_Dashboard.html /app/QQQ_0DTE_Dashboard.html

CMD ["python", "-u", "/app/python/longbridge_gateway/main.py"]
