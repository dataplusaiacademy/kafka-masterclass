FROM python:3.11-slim

WORKDIR /app

COPY python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY python/ /app/
COPY schemas/ /app/schemas/

CMD ["python", "order_producer.py"]
