import os, json, logging, time
from datetime import datetime, timedelta
from azure.eventhub import EventHubConsumerClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("batch-consumer")

CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB = os.getenv("EVENTHUB_NAME", "weather-raw")
CSV_DIR = os.path.expanduser("~/weather-pipeline/hive-data/weather")

if not CONN_STR:
    raise SystemExit("Missing EVENTHUB_CONNECTION_STRING")

# Read messages from the last hour
end_time = datetime.utcnow()
start_time = end_time - timedelta(hours=1)

client = EventHubConsumerClient.from_connection_string(
    CONN_STR, consumer_group="$Default", eventhub_name=EVENTHUB
)

def on_event(partition_context, event):
    data = event.body_as_str()
    if data:
        filename = time.strftime("weather_%Y%m%d_%H.csv")   # hourly file
        filepath = os.path.join(CSV_DIR, filename)
        with open(filepath, "a") as f:
            f.write(data + "\n")
        logger.info(f"Wrote to {filename}")
    partition_context.update_checkpoint(event)

def on_error(partition_context, error):
    logger.error(f"Error: {error}")

with client:
    logger.info("Batch consumer starting – reading events from last hour")
    client.receive(
        on_event=on_event,
        on_error=on_error,
        starting_position=start_time.isoformat()   # ISO format
    )
logger.info("Batch complete")