import os, json, logging, requests
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer-once")

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
        "timezone": "UTC"
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        cw = data["current_weather"]
        hourly = data.get("hourly", {})
        humidity = pressure = clouds = None
        if "time" in hourly:
            cw_hour = cw["time"][:13]  # match hour prefix
            for i, t in enumerate(hourly["time"]):
                if t.startswith(cw_hour):
                    humidity = hourly["relativehumidity_2m"][i]
                    pressure = hourly["pressure_msl"][i]
                    clouds = hourly["cloudcover"][i]
                    break
        return {
            "city": city,
            "timestamp": datetime.utcnow().isoformat(),
            "temp": cw["temperature"],
            "feels_like": cw["temperature"],
            "humidity": humidity,
            "pressure": pressure,
            "weather": cw.get("weathercode", 0),
            "desc": "",
            "wind": cw["windspeed"],
            "clouds": clouds,
        }
    except Exception as e:
        logger.error(f"Failed {city}: {e}")
        return None

def send_to_eventhub(data):
    producer = EventHubProducerClient.from_connection_string(CONN_STR, eventhub_name=EVENTHUB)
    producer.send_batch([EventData(json.dumps(data).encode())])
    producer.close()
    logger.info(f"Sent {data['city']}")

if __name__ == "__main__":
    for city, lat, lon in cities:
        w = fetch(city, lat, lon)
        if w:
            send_to_eventhub(w)
    logger.info("One‑shot producer finished")