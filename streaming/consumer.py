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


def compute_booking_velocity(flight_state, window_minutes=60):
    """
    Booking velocity = bookings per hour over a recent time window.
    This is our demand momentum metric.
    """
    cutoff = now() - dt.timedelta(minutes=window_minutes)
    # keep only recent bookings
    flight_state["booking_times"] = [
        t for t in flight_state["booking_times"] if t >= cutoff
    ]
    count = len(flight_state["booking_times"])
    hours = window_minutes / 60.0 if window_minutes > 0 else 1.0
    return count / hours if hours > 0 else 0.0


# ---------------------------
# Event handlers
# ---------------------------

def handle_flight_scheduled(event):
    flights[event["flight_id"]] = {
        "departure": dt.datetime.fromisoformat(
            event["scheduled_departure_time"].replace("Z", "")
        ),
        "booked_seats": 0,
        "booked_by_bucket": {"Y": 0, "M": 0, "K": 0},
        # Iteration 2 state
        "booking_times": [],
        "last_velocity": 0.0,
        "promo_sent": False,
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

    # Track booking time for momentum / velocity
    f["booking_times"].append(now())

    emit_inventory_metrics(flight_id)
    maybe_trigger_promo(flight_id)


# ---------------------------
# Emit derived events
# ---------------------------

def emit_inventory_metrics(flight_id):
    f = flights[flight_id]

    minutes_to_dep = max(
        int((f["departure"] - now()).total_seconds() / 60),
        0
    )
    days_to_dep = round(minutes_to_dep / 1440.0, 2)  # 1440 minutes in a day
    load_factor = f["booked_seats"] / TOTAL_SEATS

    velocity_per_hour = compute_booking_velocity(f, window_minutes=60)

    event = {
        "event_type": "CabinInventoryMetrics",
        "event_time": iso(now()),
        "flight_id": flight_id,
        "booked_seats": f["booked_seats"],
        "total_seats": TOTAL_SEATS,
        "load_factor": round(load_factor, 3),
        "minutes_to_departure": minutes_to_dep,
        "days_to_departure": days_to_dep,
        "booking_velocity_per_hour": round(velocity_per_hour, 3),
    }

    producer.send("inventory_metrics", event)
    print(f"📊 Metrics updated for {flight_id}")


def maybe_trigger_promo(flight_id):
    f = flights[flight_id]

    # Only one promotion per flight in Iteration 2
    if f["promo_sent"]:
        return

    minutes_to_dep = max(
        int((f["departure"] - now()).total_seconds() / 60),
        0
    )
    days_to_dep = minutes_to_dep / 1440.0
    load_factor = f["booked_seats"] / TOTAL_SEATS
    velocity_now = compute_booking_velocity(f, window_minutes=60)
    last_velocity = f.get("last_velocity", 0.0)

    # Store for next comparison
    f["last_velocity"] = velocity_now

    # Promotion window: between 2 and 7 days before departure
    in_promo_window = 2.0 <= days_to_dep <= 7.0

    # Simple demand conditions:
    # - load factor is low
    # - current velocity is low or not improving
    load_low = load_factor < 0.60
    velocity_low = velocity_now < 5.0  # synthetic threshold
    velocity_stagnant = velocity_now <= last_velocity * 0.8 or last_velocity == 0.0

    if not (in_promo_window and load_low and (velocity_low or velocity_stagnant)):
        return

    # Choose discount based on how bad the situation is
    if load_factor < 0.30:
        discount = 25
    else:
        discount = 10

    event = {
        "event_type": "PromotionTriggered",
        "event_time": iso(now()),
        "flight_id": flight_id,
        "decision_source": "RULE_ENGINE_ITERATION_2",
        "current_load_factor": round(load_factor, 3),
        "minutes_to_departure": minutes_to_dep,
        "days_to_departure": round(days_to_dep, 2),
        "booking_velocity_per_hour": round(velocity_now, 3),
        "target_fare_bucket": "K",
        "discount_percentage": discount,
    }

    producer.send("promo_decisions", event)
    f["promo_sent"] = True
    print(
        f"💸 Promo triggered for {flight_id}: {discount}% "
        f"(LF={load_factor:.2f}, days_to_dep={days_to_dep:.2f}, vel={velocity_now:.2f}/h)"
    )


# ---------------------------
# Main loop
# ---------------------------

def run():
    print("🚦 Inventory engine (Iteration 2) started")
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
