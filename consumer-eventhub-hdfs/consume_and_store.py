import os, json, time, logging, subprocess, tempfile
from azure.eventhub import EventHubConsumerClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB = os.getenv("EVENTHUB_NAME", "weather-raw")
HDFS_TARGET = "/user/weather/raw/"
BATCH_SIZE = 5   # write after every 5 events
CHECKPOINT_INTERVAL = 30  # seconds

def save_to_hdfs(events):
    """Write a batch of events to HDFS as a CSV file."""
    # Create a temporary local file
    fd, tmp_path = tempfile.mkstemp(suffix=".csv", prefix="weather-")
    with os.fdopen(fd, "w") as f:
        for event in events:
            f.write(event + "\n")
    
    # Upload to HDFS
    hdfs_path = f"{HDFS_TARGET}weather_{int(time.time())}.csv"
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", tmp_path, hdfs_path],
        check=True, capture_output=True, text=True
    )
    logger.info(f"Uploaded {len(events)} events to {hdfs_path}")
    os.unlink(tmp_path)

def on_event(partition_context, event):
    # This callback is called per event; we'll batch externally
    global batch
    if event.body_as_str():
        batch.append(event.body_as_str())
        if len(batch) >= BATCH_SIZE:
            save_to_hdfs(batch.copy())
            batch.clear()
    partition_context.update_checkpoint(event)

def on_error(partition_context, error):
    logger.error(f"Error: {error}")

if __name__ == "__main__":
    if not CONN_STR:
        raise SystemExit("Missing EVENTHUB_CONNECTION_STRING")
    
    batch = []
    client = EventHubConsumerClient.from_connection_string(
        CONN_STR,
        consumer_group="$Default",
        eventhub_name=EVENTHUB,
    )
    
    logger.info("Starting Event Hub consumer → HDFS")
    with client:
        client.receive(
            on_event=on_event,
            on_error=on_error,
            starting_position="-1",  # from beginning (for test)
        )