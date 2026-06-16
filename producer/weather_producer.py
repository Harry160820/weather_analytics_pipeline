import os, json, time, logging
from datetime import datetime
import logging
import requests
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData    

# Load environment variables
load_dotenv("azure/config.env")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Map WMO weather codes to human‑readable descriptions

WMO_CODES = {
    0: "Clear sky",
    1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    56: "Light freezing drizzle", 57: "Dense freezing drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    66: "Light freezing rain", 67: "Heavy freezing rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail"
}

class WeatherProducer:
    def __init__(self):
        self.conn_str  = os.getenv("EVENTHUB_CONNECTION_STRING")
        self.eventhub = os.getenv("EVENTHUB_NAME", "weather-raw")
        if not self.conn_str:
            raise SystemExit("Missing env var: EVENTHUB_CONNECTION_STRING")
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=self.conn_str , 
            eventhub_name=self.eventhub
            )
        
        # 5 indian metro cities with their latitudes and longitudes
        self.cities = [
            ("Mumbai", 19.076, 72.8777),
            ("Delhi", 28.6139, 77.209),
            ("Bangalore", 12.9716, 77.5946),
            ("Hyderabad", 17.385, 78.4867),
            ("Chennai", 13.0827, 80.2707),
        ]
        self.sleep_sec = 30 # 30 seconds

    def fetch(self, city, lat, lon):
        #fetch current weather from open‑meteo
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "current_weather": True,
            "hourly": "relativehumidity_2m,pressure_msl,cloudcover",
            "timezone": "Asia/Kolkata"
        }

        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            cw = data["current_weather"]
            hourly = data.get("hourly", {})
            # Extract current hour index for additional data
            current_time = cw["time"]
            idx = None
            if "time" in hourly:
                try:
                    idx = hourly["time"].index(current_time)
                except ValueError:
                    #find closest hour
                    times = hourly["time"]

                    closest = min(
                        times, 
                        key=lambda x: abs(
                            datetime.fromisoformat(x)-
                            datetime.fromisoformat(current_time)
                        )
                    )
                    idx = times.index(closest)

            return  {
                "city": city,
                "timestamp": datetime.utcnow().isoformat(),
                "temp": cw["temperature"],
                "feels_like": cw["temperature"],           # Open‑Meteo doesn't give feels‑like
                "humidity": hourly["relativehumidity_2m"][idx] if idx is not None else None,
                "pressure": hourly["pressure_msl"][idx] if idx is not None else None,
                "weather": WMO_CODES.get(cw["weathercode"], "Unknown"),
                "desc": WMO_CODES.get(cw["weathercode"], "Unknown"),
                "wind": cw["windspeed"],
                "clouds": hourly["cloudcover"][idx] if idx else None
            }
        except Exception as e:
            logging.error(f"Error fetching weather for {city}: {e}")
            return None
    
    def send(self, data):
        self.producer.send_batch([EventData(json.dumps(data).encode())])
        logging.info(f"Sent weather data for {data['city']} at {data['timestamp']}")

    def run(self):
        logging.info(f"starting weather producer..., interval={self.sleep_sec}s")
        while True:
            for city, lat, lon in self.cities:
                w = self.fetch(city, lat, lon)
                if w:
                    self.send(w)
                time.sleep(2)
            logging.info(f"Cycle done, Sleeping for {self.sleep_sec} seconds...")
            time.sleep(self.sleep_sec)

if __name__ == "__main__":
    if not os.getenv("EVENTHUB_CONNECTION_STRING"):
        raise SystemExit("Missing env var: EVENTHUB_CONNECTION_STRING")
    WeatherProducer().run()
