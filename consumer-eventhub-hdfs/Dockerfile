FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY consume_and_store.py .

CMD ["python", "consume_and_store.py"]