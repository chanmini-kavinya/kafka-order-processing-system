FROM python:3.9-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/producer.py /app/producer.py
COPY schemas /app/schemas
COPY config.py /app/config.py

CMD ["python", "producer.py"]
