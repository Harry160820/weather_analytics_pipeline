import os, json, time, logging, sys
from datetime import datetime
import requests
from azure.eventhub import EventHubProducerClient, EventData

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer-once")

# Configuration from environment
CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB = os.getenv("EVENTHUB_NAME", "weather-raw")

cities = [
    ("Mumbai", 19.076, 72.8777),
    ("Delhi", 28.6139, 77.209),
    ("Bangalore", 12.9716, 77.5946),
    ("Hyderabad", 17.385, 78.4867),
    ("Chennai", 13.0827, 80.2707),
]

def fetch(city, lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "current_weather": True,
        "hourly": "relativehumidity_2m,pressure_msl,cloudcover",
        "timezone": "Asia/Kolkata"
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    d = r.json()
    cw = d["current_weather"]
    hourly = d.get("hourly", {})
    idx = hourly["time"].index(cw["time"]) if "time" in hourly else None
    return {
        "city": city,
        "timestamp": datetime.utcnow().isoformat(),
        "temp": cw["temperature"],
        "feels_like": cw["temperature"],
        "humidity": hourly["relativehumidity_2m"][idx] if idx else None,
        "pressure": hourly["pressure_msl"][idx] if idx else None,
        "weather": cw.get("weathercode", 0),
        "desc": "",
        "wind": cw["windspeed"],
        "clouds": hourly["cloudcover"][idx] if idx else None
    }

def send_to_eventhub(data):
    producer = EventHubProducerClient.from_connection_string(CONN_STR, eventhub_name=EVENTHUB)
    producer.send_batch([EventData(json.dumps(data).encode())])
    logger.info(f"Sent {data['city']}")
    producer.close()

if __name__ == "__main__":
    for city, lat, lon in cities:
        try:
            w = fetch(city, lat, lon)
            if w:
                send_to_eventhub(w)
        except Exception as e:
            logger.error(f"Failed {city}: {e}")
    logger.info("One‑shot producer finished")