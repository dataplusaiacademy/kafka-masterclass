FROM python:3.12-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY seed_orders.py consume_cdc_topic.py ./

ENTRYPOINT []
CMD ["python", "-c", "print('Run: docker compose --profile tools run --rm fake-seed | topic-consumer (see README)')"]
