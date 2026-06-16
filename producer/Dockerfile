FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer/weather_producer.py .

CMD ["python", "weather_producer.py"]