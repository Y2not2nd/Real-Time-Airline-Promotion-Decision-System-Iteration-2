import json
import datetime as dt
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"

consumer = KafkaConsumer(
    "flight_lifecycle",
    "bookings",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="inventory-engine",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------------------------
# In-memory state
# ---------------------------

flights = {}
TOTAL_SEATS = 180

# ---------------------------
# Helper functions
# ---------------------------

def iso(ts):
    return ts.isoformat() + "Z"

def now():
    return dt.datetime.utcnow()

# ---------------------------
# Event handlers
# ---------------------------

def handle_flight_scheduled(event):
    flights[event["flight_id"]] = {
        "departure": dt.datetime.fromisoformat(
            event["scheduled_departure_time"].replace("Z", "")
        ),
        "booked_seats": 0,
        "booked_by_bucket": {"Y": 0, "M": 0, "K": 0}
    }
    print(f"✈️ Tracking flight {event['flight_id']}")

def handle_booking(event):
    flight_id = event["flight_id"]
    if flight_id not in flights:
        return

    f = flights[flight_id]
    f["booked_seats"] += len(event["passengers"])
    bucket = event["fare_bucket"]
    f["booked_by_bucket"][bucket] += len(event["passengers"])

    emit_inventory_metrics(flight_id)
    maybe_trigger_promo(flight_id)

# ---------------------------
# Emit derived events
# ---------------------------

def emit_inventory_metrics(flight_id):
    f = flights[flight_id]
    minutes_to_dep = max(
        int((f["departure"] - now()).total_seconds() / 60), 0
    )
    load_factor = f["booked_seats"] / TOTAL_SEATS

    event = {
        "event_type": "CabinInventoryMetrics",
        "event_time": iso(now()),
        "flight_id": flight_id,
        "booked_seats": f["booked_seats"],
        "total_seats": TOTAL_SEATS,
        "load_factor": round(load_factor, 3),
        "minutes_to_departure": minutes_to_dep
    }

    producer.send("inventory_metrics", event)
    print(f"📊 Metrics updated for {flight_id}")

def maybe_trigger_promo(flight_id):
    f = flights[flight_id]
    minutes_to_dep = max(
        int((f["departure"] - now()).total_seconds() / 60), 0
    )
    load_factor = f["booked_seats"] / TOTAL_SEATS

    discount = None

    if minutes_to_dep < 90 and load_factor < 0.70:
        discount = 10
    if minutes_to_dep < 45 and load_factor < 0.80:
        discount = 25

    if discount:
        event = {
            "event_type": "PromotionTriggered",
            "event_time": iso(now()),
            "flight_id": flight_id,
            "decision_source": "RULE_ENGINE",
            "current_load_factor": round(load_factor, 3),
            "minutes_to_departure": minutes_to_dep,
            "target_fare_bucket": "K",
            "discount_percentage": discount
        }

        producer.send("promo_decisions", event)
        print(f"💸 Promo triggered for {flight_id}: {discount}%")

# ---------------------------
# Main loop
# ---------------------------

def run():
    print("🚦 Inventory engine started")
    for msg in consumer:
        event = msg.value
        et = event.get("event_type")

        if et == "FlightScheduled":
            handle_flight_scheduled(event)
        elif et == "BookingCreated":
            handle_booking(event)

# ---------------------------
if __name__ == "__main__":
    run()
