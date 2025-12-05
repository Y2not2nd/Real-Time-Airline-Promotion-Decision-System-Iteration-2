
import json
import time
import uuid
import random
import datetime as dt
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------------------------
# Configuration
# ---------------------------

FLIGHT_COUNT = 3               # how many flights to simulate
SIMULATION_SPEED = 60          # 1 loop = 1 simulated minute
BOOKING_PROB_BASE = 0.05       # base booking probability

# ---------------------------
# Helper functions
# ---------------------------

def now():
    return dt.datetime.utcnow()

def iso(ts):
    return ts.isoformat() + "Z"

def generate_flights():
    flights = []
    start_time = now() + dt.timedelta(minutes=30)

    for i in range(FLIGHT_COUNT):
        flight_id = f"BA21{i}_{start_time.date()}"
        departure = start_time + dt.timedelta(minutes=i * 60)
        arrival = departure + dt.timedelta(minutes=75)

        flights.append({
            "flight_id": flight_id,
            "departure": departure,
            "arrival": arrival,
            "total_seats": 180,
            "booked": 0,
            "fare_buckets": {
                "Y": {"price": 220, "seats": 20},
                "M": {"price": 180, "seats": 40},
                "K": {"price": 140, "seats": 120},
            },
            "seat_map": {},
        })

    return flights

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
    event = {
        "event_type": "CabinInventorySnapshot",
        "event_time": iso(now()),
        "flight_id": flight["flight_id"],
        "time_to_departure_minutes": max(
            int((flight["departure"] - now()).total_seconds() / 60), 0
        ),
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
# Simulation loop
# ---------------------------

def run():
    flights = generate_flights()

    for flight in flights:
        send_flight_scheduled(flight)

    print("✈️ Flights scheduled")

    while True:
        for flight in flights:
            minutes_to_departure = (
                flight["departure"] - now()
            ).total_seconds() / 60

            if minutes_to_departure <= 0:
                continue

            # increase booking probability closer to departure
            prob = BOOKING_PROB_BASE * (
                1 + (120 - minutes_to_departure) / 60
            )

            if random.random() < prob:
                send_booking(flight)

            send_inventory_snapshot(flight)

        time.sleep(1)

# ---------------------------
if __name__ == "__main__":
    run()
