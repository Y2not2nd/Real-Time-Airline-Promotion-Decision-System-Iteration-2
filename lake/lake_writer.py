import json
import os
import datetime as dt
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    "flight_lifecycle",
    "bookings",
    "seat_inventory",
    "seat_map",
    "inventory_metrics",
    "promo_decisions"
]

BASE_PATH = "lake/raw"

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="lake-writer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest"
)

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def write_event(topic, event):
    date_str = dt.date.today().isoformat()
    topic_path = os.path.join(BASE_PATH, topic)
    ensure_dir(topic_path)

    file_path = os.path.join(topic_path, f"{date_str}.jsonl")

    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")

def run():
    print("🪣 Lake ingestion started")
    for msg in consumer:
        write_event(msg.topic, msg.value)

if __name__ == "__main__":
    run()
