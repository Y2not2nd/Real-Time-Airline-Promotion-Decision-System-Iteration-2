import polars as pl
from pathlib import Path
import psycopg2

RAW = Path("lake/raw")
GOLD = Path("lake/gold")
GOLD.mkdir(parents=True, exist_ok=True)

TS_FORMAT = "%Y-%m-%dT%H:%M:%S%.fZ"

# -----------------------
# Helpers
# -----------------------

def scan(topic: str):
    files = list((RAW / topic).glob("*.jsonl"))
    if not files:
        return None
    return pl.scan_ndjson(files)

def parse_utc(col: pl.Expr) -> pl.Expr:
    """
    Parse ISO-8601 UTC timestamps as naive datetimes.
    We assume everything is already in UTC (trailing 'Z').
    """
    return col.str.strptime(pl.Datetime, format=TS_FORMAT)

# -----------------------
# DIM FLIGHT
# -----------------------

def build_dim_flight():
    df = scan("flight_lifecycle")
    if df is None:
        return None

    dim = (
        df.filter(pl.col("event_type") == "FlightScheduled")
          .select(
              "flight_id",
              "airline_code",
              "origin_airport",
              "destination_airport",
              parse_utc(pl.col("scheduled_departure_time")).alias("scheduled_departure_time"),
              parse_utc(pl.col("scheduled_arrival_time")).alias("scheduled_arrival_time"),
              "aircraft_type",
              "total_seats"
          )
          .unique("flight_id")
    )

    dim.collect().write_parquet(GOLD / "dim_flight.parquet")
    return dim

# -----------------------
# FACT BOOKINGS
# -----------------------

def build_fact_booking():
    df = scan("bookings")
    if df is None:
        return None

    fact = (
        df.filter(pl.col("event_type") == "BookingCreated")
          .with_columns(
              parse_utc(pl.col("event_time")).alias("booking_time")
          )
          .explode("passengers")
          .with_columns(
              pl.col("passengers").struct.field("seat_id").alias("seat_id")
          )
          .select(
              "booking_time",
              "booking_id",
              "flight_id",
              "fare_bucket",
              "seat_id",
              "total_price"
          )
    )

    fact.collect().write_parquet(GOLD / "fact_booking.parquet")
    return fact

# -----------------------
# FACT INVENTORY METRICS
# -----------------------

def build_fact_inventory():
    df = scan("inventory_metrics")
    if df is None:
        return None

    fact = (
        df.with_columns(
            parse_utc(pl.col("event_time")).alias("metric_time")
        )
        .select(
            "metric_time",
            "flight_id",
            "booked_seats",
            "total_seats",
            "load_factor",
            "minutes_to_departure"
        )
    )

    fact.collect().write_parquet(GOLD / "fact_inventory_metrics.parquet")
    return fact

# -----------------------
# FACT PROMOTIONS
# -----------------------

def build_fact_promotions():
    df = scan("promo_decisions")
    if df is None:
        return None

    fact = (
        df.with_columns(
            parse_utc(pl.col("event_time")).alias("promo_time")
        )
        .select(
            "promo_time",
            "flight_id",
            "decision_source",
            "discount_percentage",
            "current_load_factor",
            "minutes_to_departure"
        )
    )

    fact.collect().write_parquet(GOLD / "fact_promotions.parquet")
    return fact

# -----------------------
# LOAD TO POSTGRES
# -----------------------

def load_to_postgres(table_name: str, df: pl.LazyFrame):
    pdf = df.collect()

    conn = psycopg2.connect(
        dbname="airline_dw",
        user="airline",
        password="airline",
        host="localhost"
    )
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

    columns = ", ".join(f"{c} TEXT" for c in pdf.columns)
    cur.execute(f"CREATE TABLE {table_name} ({columns})")

    for row in pdf.rows():
        placeholders = ", ".join(["%s"] * len(row))
        cur.execute(
            f"INSERT INTO {table_name} VALUES ({placeholders})",
            tuple(map(str, row))
        )

    conn.commit()
    conn.close()

# -----------------------
# MAIN
# -----------------------

def run():
    tables = {
        "dim_flight": build_dim_flight(),
        "fact_booking": build_fact_booking(),
        "fact_inventory_metrics": build_fact_inventory(),
        "fact_promotions": build_fact_promotions(),
    }

    for name, df in tables.items():
        if df is not None:
            print(f"📦 Writing {name}")
            load_to_postgres(name, df)

if __name__ == "__main__":
    run()
