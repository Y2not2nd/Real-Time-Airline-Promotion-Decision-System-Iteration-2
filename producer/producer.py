import json
import time
import uuid
import random
import datetime as dt
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Consumer to listen for promotion decisions from the streaming engine
promo_consumer = KafkaConsumer(
    "promo_decisions",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="airline-producer-feedback",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ---------------------------
# Configuration
# ---------------------------

# More flights for Iteration 2
FLIGHT_COUNT = 50                 # how many flights to simulate

# Base booking probability, before time / promo factors
BOOKING_PROB_BASE = 0.02

TOTAL_SEATS = 180


# ---------------------------
# Helper functions
# ---------------------------

def now():
    return dt.datetime.utcnow()


def iso(ts):
    return ts.isoformat() + "Z"


def generate_flights():
    """
    Generate flights with departures spread between 2 and 7 days from now.
    This matches the promotion window used in Iteration 2.
    """
    flights = []
    base = now()

    for i in range(FLIGHT_COUNT):
        # departure between 2 and 7 days from now, random hour
        days_ahead = random.uniform(2, 7)
        departure = base + dt.timedelta(days=days_ahead, hours=random.randint(0, 23))
        arrival = departure + dt.timedelta(minutes=90)

        flight_id = f"BA2{i:02d}_{departure.date()}"

        flights.append({
            "flight_id": flight_id,
            "departure": departure,
            "arrival": arrival,
            "total_seats": TOTAL_SEATS,
            "booked": 0,
            "fare_buckets": {
                "Y": {"price": 220, "seats": 20},
                "M": {"price": 180, "seats": 40},
                "K": {"price": 140, "seats": 120},
            },
            "seat_map": {},
            # Iteration 2 additions
            "promotion_active": False,
            "current_discount": 0.0,
        })

    return flights


def days_to_departure(flight):
    return max(
        (flight["departure"] - now()).total_seconds() / 86400.0,
        0.0
    )


def time_pressure_factor(days):
    """
    Simple, explainable curve:
      - Far from departure -> lower urgency
      - Closer to departure (in days) -> higher booking pressure
    """
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


def promo_factor(discount_percentage):
    """
    Promotions increase the chance of booking, they do not guarantee it.
    """
    if discount_percentage >= 25:
        return 1.35
    elif discount_percentage >= 10:
        return 1.15
    else:
        return 1.0


# ---------------------------
# Event emission
# ---------------------------

def send_flight_scheduled(flight):
    event = {
        "event_type": "FlightScheduled",
        "event_time": iso(now()),
        "flight_id": flight["flight_id"],
        "airline_code": "BA",
        "origin_airport": "LHR",
        "destination_airport": "AMS",
        "scheduled_departure_time": iso(flight["departure"]),
        "scheduled_arrival_time": iso(flight["arrival"]),
        "aircraft_type": "A320",
        "total_seats": flight["total_seats"],
        "cabins": [
            {"cabin_code": "Y", "seat_count": 150},
            {"cabin_code": "J", "seat_count": 30}
        ]
    }
    producer.send("flight_lifecycle", event)


def send_inventory_snapshot(flight):
    minutes_to_departure = max(
        int((flight["departure"] - now()).total_seconds() / 60),
        0
    )
    event = {
        "event_type": "CabinInventorySnapshot",
        "event_time": iso(now()),
        "flight_id": flight["flight_id"],
        "time_to_departure_minutes": minutes_to_departure,
        "cabins": [{
            "cabin_code": "Y",
            "fare_buckets": [
                {
                    "bucket": k,
                    "available_seats": v["seats"],
                    "price": v["price"]
                }
                for k, v in flight["fare_buckets"].items()
            ]
        }],
        "currency": "GBP"
    }
    producer.send("seat_inventory", event)


def send_booking(flight):
    bucket = random.choices(
        population=["Y", "M", "K"],
        weights=[1, 3, 6],
        k=1
    )[0]

    # No seats left in this bucket
    if flight["fare_buckets"][bucket]["seats"] <= 0:
        return

    flight["fare_buckets"][bucket]["seats"] -= 1
    flight["booked"] += 1

    event = {
        "event_type": "BookingCreated",
        "event_time": iso(now()),
        "booking_id": str(uuid.uuid4()),
        "flight_id": flight["flight_id"],
        "channel": "WEB",
        "fare_bucket": bucket,
        "currency": "GBP",
        "total_price": flight["fare_buckets"][bucket]["price"],
        "passengers": [{
            "passenger_id": "P1",
            "seat_id": f"{random.randint(1,30)}{random.choice('ABCDEF')}",
            "cabin_code": "Y"
        }]
    }
    producer.send("bookings", event)


# ---------------------------
# Promotion feedback handling
# ---------------------------

def handle_promo_event(event, flights):
    """
    When the streaming consumer fires a PromotionTriggered event,
    this updates the corresponding flight's promotion state.

    This is the feedback loop: promotions now influence booking probability.
    """
    flight_id = event.get("flight_id")
    discount = event.get("discount_percentage", 0)

    if not flight_id:
        return

    for f in flights:
        if f["flight_id"] == flight_id:
            f["promotion_active"] = True
            f["current_discount"] = float(discount)
            print(f"💡 Promotion applied to {flight_id}: {discount}%")
            break


def poll_promotions(flights):
    """
    Non-blocking poll of the promo_decisions topic.
    """
    records = promo_consumer.poll(timeout_ms=100)
    for _, msgs in records.items():
        for msg in msgs:
            event = msg.value
            if event.get("event_type") == "PromotionTriggered":
                handle_promo_event(event, flights)


# ---------------------------
# Simulation loop
# ---------------------------

def run():
    flights = generate_flights()

    for flight in flights:
        send_flight_scheduled(flight)

    print(f"✈️ Flights scheduled for Iteration 2: {len(flights)} flights")

    while True:
        # First, apply any new promotions coming from the streaming engine
        poll_promotions(flights)

        for flight in flights:
            # If all seats are sold, we still send snapshots but no more bookings
            if flight["booked"] >= flight["total_seats"]:
                send_inventory_snapshot(flight)
                continue

            dtd = days_to_departure(flight)

            # Booking probability is a product of:
            # - base rate
            # - time pressure factor (days to departure)
            # - promotion factor (discount percentage)
            time_factor = time_pressure_factor(dtd)

            if flight["promotion_active"]:
                promo_mult = promo_factor(flight["current_discount"])
            else:
                promo_mult = 1.0

            prob = BOOKING_PROB_BASE * time_factor * promo_mult

            # Keep probability in a sensible range
            prob = max(min(prob, 0.8), 0.0)

            if random.random() < prob:
                send_booking(flight)

            send_inventory_snapshot(flight)

        # 1 second wall time between loops is fine, we are not simulating real wall-clock days
        time.sleep(1)


# ---------------------------
if __name__ == "__main__":
    run()
