import os, json, logging, time
from datetime import datetime, timedelta
from azure.eventhub import EventHubConsumerClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("batch-consumer")

CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB = os.getenv("EVENTHUB_NAME", "weather-raw")
CSV_DIR = "/data/weather"

if not CONN_STR:
    raise SystemExit("Missing EVENTHUB_CONNECTION_STRING")

os.makedirs(CSV_DIR, exist_ok=True)

batch = []
MAX_EVENTS = 20
STOP_TIME = time.time() + 15   # stop after 15 seconds no matter what

def on_event(partition_context, event):
    data = event.body_as_str()
    if data:
        batch.append(data)
        logger.info(f"Received event: {data[:60]}...")
    partition_context.update_checkpoint(event)
    # Stop early if we have enough events or time exceeded
    if len(batch) >= MAX_EVENTS or time.time() > STOP_TIME:
        client.close()

def on_error(partition_context, error):
    logger.error(f"Error: {error}")
    client.close()

client = EventHubConsumerClient.from_connection_string(
    CONN_STR, consumer_group="$Default", eventhub_name=EVENTHUB
)

with client:
    logger.info("Consumer started – will stop after 15 seconds or 20 events")
    try:
        client.receive(
            on_event=on_event,
            on_error=on_error,
            starting_position="-1",   # read all messages from the beginning of the partition
            max_batch_size=10,
            max_wait_time=5
        )
    except Exception as e:
        logger.error(f"Receive loop ended: {e}")

# Write all collected events to CSV
filename = time.strftime("weather_%Y%m%d_%H.csv")
filepath = os.path.join(CSV_DIR, filename)
if batch:
    with open(filepath, "a") as f:
        for event in batch:
            f.write(event + "\n")
    logger.info(f"Wrote {len(batch)} events to {filename}")
else:
    # Write a dummy line so Hive sees the file isn't empty (optional)
    with open(filepath, "a") as f:
        f.write("no_events_placeholder\n")
    logger.info(f"No events found. Wrote placeholder to {filename}.")
