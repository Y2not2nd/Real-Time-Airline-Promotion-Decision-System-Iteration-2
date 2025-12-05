import json
import time
import uuid
import random
import datetime as dt
from kafka import KafkaProducer, KafkaConsumer

from prometheus_client import (
    start_http_server,
    Counter,
    Gauge
)

BOOTSTRAP_SERVERS = "localhost:9092"

# ---------------------------
# Prometheus metrics
# ---------------------------

BOOKINGS_EMITTED = Counter(
    "producer_bookings_emitted_total",
    "Total booking events emitted"
)

PROMOS_APPLIED = Counter(
    "producer_promotions_applied_total",
    "Total promotions applied to flights"
)

BOOKING_PROBABILITY = Gauge(
    "producer_booking_probability",
    "Current booking probability",
    ["flight_id"]
)

ACTIVE_PROMOTIONS = Gauge(
    "producer_active_promotions",
    "Number of active promotions"
)

# ---------------------------
# Kafka setup
# ---------------------------

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

promo_consumer = KafkaConsumer(
    "promo_decisions",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="airline-producer-feedback",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ---------------------------
# Configuration
# ---------------------------

FLIGHT_COUNT = 40
BOOKING_PROB_BASE = 0.02
TOTAL_SEATS = 180

# ---------------------------
# Helper functions
# ---------------------------

def now():
    return dt.datetime.utcnow()


def iso(ts):
    return ts.isoformat() + "Z"


def days_to_departure(flight):
    return max(
        (flight["departure"] - now()).total_seconds() / 86400.0,
        0.0
    )


def time_pressure_factor(days):
    if days > 10:
        return 0.5
    elif days > 7:
        return 0.7
    elif days > 4:
        return 1.0
    elif days >= 2:
        return 1.3
    else:
        return 1.6


def promo_factor(discount):
    if discount >= 25:
        return 1.35
    elif discount >= 10:
        return 1.15
    return 1.0

# ---------------------------
# Flight generation
# ---------------------------

def generate_flights():
    flights = []
    base = now()

    for i in range(FLIGHT_COUNT):
        departure = base + dt.timedelta(
            days=random.uniform(2, 7),
            hours=random.randint(0, 23)
        )

        flights.append({
            "flight_id": f"BA2{i:02d}_{departure.date()}",
            "departure": departure,
            "arrival": departure + dt.timedelta(minutes=90),
            "total_seats": TOTAL_SEATS,
            "booked": 0,
            "fare_buckets": {
                "Y": {"price": 220, "seats": 20},
                "M": {"price": 180, "seats": 40},
                "K": {"price": 140, "seats": 120},
            },
            "promotion_active": False,
            "current_discount": 0.0,
        })

    return flights

# ---------------------------
# Event emission
# ---------------------------

def send_booking(flight):
    bucket = random.choices(["Y", "M", "K"], weights=[1, 3, 6])[0]
    if flight["fare_buckets"][bucket]["seats"] <= 0:
        return

    flight["fare_buckets"][bucket]["seats"] -= 1
    flight["booked"] += 1

    event = {
        "event_type": "BookingCreated",
        "event_time": iso(now()),
        "booking_id": str(uuid.uuid4()),
        "flight_id": flight["flight_id"],
        "fare_bucket": bucket,
        "total_price": flight["fare_buckets"][bucket]["price"],
        "passengers": [{"passenger_id": "P1"}],
    }

    producer.send("bookings", event)
    BOOKINGS_EMITTED.inc()


def handle_promo_event(event, flights):
    for f in flights:
        if f["flight_id"] == event["flight_id"]:
            f["promotion_active"] = True
            f["current_discount"] = float(event["discount_percentage"])
            PROMOS_APPLIED.inc()
            ACTIVE_PROMOTIONS.set(
                sum(1 for x in flights if x["promotion_active"])
            )

# ---------------------------
# Main loop
# ---------------------------

def run():
    print("🚀 Producer started (Iteration 2 with metrics)")
    flights = generate_flights()

    # Start Prometheus endpoint
    start_http_server(8001)

    while True:
        records = promo_consumer.poll(timeout_ms=100)
        for _, msgs in records.items():
            for msg in msgs:
                handle_promo_event(msg.value, flights)

        for f in flights:
            if f["booked"] >= f["total_seats"]:
                continue

            dtd = days_to_departure(f)
            prob = BOOKING_PROB_BASE * time_pressure_factor(dtd)

            if f["promotion_active"]:
                prob *= promo_factor(f["current_discount"])

            prob = min(prob, 0.8)
            BOOKING_PROBABILITY.labels(f["flight_id"]).set(prob)

            if random.random() < prob:
                send_booking(f)

        time.sleep(1)


if __name__ == "__main__":
    run()
