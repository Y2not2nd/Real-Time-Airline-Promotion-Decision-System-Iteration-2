import json
import datetime as dt
from kafka import KafkaConsumer, KafkaProducer

from prometheus_client import (
    start_http_server,
    Counter,
    Gauge
)

BOOTSTRAP_SERVERS = "localhost:9092"

# ---------------------------
# Prometheus metrics
# ---------------------------

PROMOTIONS_TRIGGERED = Counter(
    "consumer_promotions_triggered_total",
    "Total promotions triggered"
)

BOOKING_VELOCITY = Gauge(
    "consumer_booking_velocity_per_hour",
    "Booking velocity per flight",
    ["flight_id"]
)

FLIGHTS_TRACKED = Gauge(
    "consumer_flights_tracked",
    "Number of active flights tracked"
)

# ---------------------------
# Kafka
# ---------------------------

consumer = KafkaConsumer(
    "flight_lifecycle",
    "bookings",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="inventory-engine",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------------------------
# State
# ---------------------------

flights = {}
TOTAL_SEATS = 180

def now():
    return dt.datetime.utcnow()

def iso(ts):
    return ts.isoformat() + "Z"

def booking_velocity(state, minutes=60):
    cutoff = now() - dt.timedelta(minutes=minutes)
    state["booking_times"] = [t for t in state["booking_times"] if t >= cutoff]
    return len(state["booking_times"]) / (minutes / 60)

# ---------------------------
# Handlers
# ---------------------------

def handle_flight(event):
    flights[event["flight_id"]] = {
        "departure": dt.datetime.fromisoformat(
            event["scheduled_departure_time"].replace("Z", "")
        ),
        "booked": 0,
        "booking_times": [],
        "promo_sent": False,
    }
    FLIGHTS_TRACKED.set(len(flights))

def handle_booking(event):
    f = flights[event["flight_id"]]
    f["booked"] += 1
    f["booking_times"].append(now())

    velocity = booking_velocity(f)
    BOOKING_VELOCITY.labels(event["flight_id"]).set(velocity)

    days = (f["departure"] - now()).total_seconds() / 86400

    if not f["promo_sent"] and 2 <= days <= 7 and velocity < 5:
        producer.send("promo_decisions", {
            "event_type": "PromotionTriggered",
            "event_time": iso(now()),
            "flight_id": event["flight_id"],
            "discount_percentage": 10
        })
        PROMOTIONS_TRIGGERED.inc()
        f["promo_sent"] = True

# ---------------------------
# Main
# ---------------------------

def run():
    print("🚦 Consumer started (Iteration 2 with metrics)")
    start_http_server(8002)

    for msg in consumer:
        e = msg.value
        if e["event_type"] == "FlightScheduled":
            handle_flight(e)
        elif e["event_type"] == "BookingCreated":
            handle_booking(e)

if __name__ == "__main__":
    run()
