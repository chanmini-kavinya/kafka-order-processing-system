FROM python:3.9-slim

WORKDIR /app

COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer/consumer.py /app/consumer.py
COPY schemas /app/schemas
COPY config.py /app/config.py

CMD ["python", "-u", "consumer.py"]
