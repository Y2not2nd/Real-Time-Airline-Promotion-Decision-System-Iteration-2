
# RUNBOOK, Airline Streaming Analytics, Iteration 2

This runbook describes how to start, verify, and validate the full Iteration 2 system end to end.

The system is a simulated real time airline analytics pipeline built on Kafka, Python streaming, a file based lake, batch ETL, PostgreSQL, and Prometheus observability.

---

## System overview

This project consists of the following components running together:

Infrastructure

* Zookeeper
* Kafka
* PostgreSQL
* Redis, present but unused in Iteration 2
* Prometheus
* Grafana
* ETL runner container

Long running application processes

* Lake writer
* Streaming consumer
* Producer

All components must be running concurrently for the system to behave correctly.

---

## Prerequisites

You must have the following installed locally:

* Docker Desktop
* Python 3.10 or later
* Git
* Ports available on the host:

  * 2181, Zookeeper
  * 9092, Kafka
  * 5432, PostgreSQL
  * 6379, Redis
  * 9090, Prometheus
  * 3000, Grafana
  * 8001, Producer metrics
  * 8002, Consumer metrics

---

## Step 1, clone repository

```bash
git clone <repository-url>
cd airline-streaming-project-iteration-2
```

Ensure the working tree is clean.

```bash
git status
```

---

## Step 2, start infrastructure services

From the repository root:

```bash
docker compose up -d
```

Wait until containers are running.

Verify with:

```bash
docker ps
```

You should see at minimum:

* zookeeper
* kafka
* postgres
* redis
* prometheus
* grafana
* etl-runner

If any container repeatedly exits, inspect logs before proceeding.

---

## Step 3, create Kafka topics

Kafka topics are not auto created.

Open a shell inside the Kafka container:

```bash
docker exec -it kafka bash
```

Create required topics:

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic flight_lifecycle --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --topic bookings --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --topic seat_inventory --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --topic inventory_metrics --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --create --topic promo_decisions --partitions 3 --replication-factor 1
```

Verify:

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

Exit the container:

```bash
exit
```

---

## Step 4, start application processes

These processes **must run concurrently**, each in its own terminal.

### Terminal 1, Lake writer

```bash
python lake/lake_writer.py
```

Expected output:

```
🪣 Lake ingestion started
```

Leave this running.

---

### Terminal 2, Streaming consumer

```bash
python streaming/consumer.py
```

Expected output:

```
🚦 Consumer started (Iteration 2 with metrics)
```

Leave this running.

---

### Terminal 3, Producer

```bash
python producer/airline_producer.py
```

Expected output:

```
🚀 Producer started (Iteration 2 with metrics)
```

Leave this running.

---

## Step 5, verify raw data lake

After 1 to 2 minutes, confirm raw events are being written.

```bash
ls lake/raw
```

Expected folders:

* flight_lifecycle
* bookings
* seat_inventory
* inventory_metrics
* promo_decisions

Inspect a sample file:

```bash
ls lake/raw/bookings
Get-Content lake/raw/bookings/*.jsonl | Select-Object -First 3
```

You should see JSON events.

---

## Step 6, verify ETL execution

The ETL runner executes automatically every 30 minutes.

To verify manually after the first run, check:

```bash
ls lake/gold
```

Expected files:

* dim_flight.parquet
* fact_booking.parquet
* fact_inventory_metrics.parquet
* fact_promotions.parquet

---

## Step 7, verify PostgreSQL tables

Connect to Postgres:

```bash
docker exec -it postgres psql -U airline -d airline_dw
```

Inside psql:

```sql
\dt

SELECT COUNT(*) FROM dim_flight;
SELECT COUNT(*) FROM fact_booking;
SELECT COUNT(*) FROM fact_inventory_metrics;
SELECT COUNT(*) FROM fact_promotions;

\q
```

All tables should exist and have non zero row counts after sufficient runtime.

---

## Step 8, verify Prometheus scraping

Open Prometheus UI:

[http://localhost:9090](http://localhost:9090)

Navigate to **Status → Targets**.

Confirm:

* airline_producer target is UP
* airline_consumer target is UP

If a target is DOWN, verify:

* producer metrics on [http://localhost:8001/metrics](http://localhost:8001/metrics)
* consumer metrics on [http://localhost:8002/metrics](http://localhost:8002/metrics)

---

## Step 9, optional Kafka stream inspection

To inspect live events, for example bookings:

```bash
docker exec -it kafka bash -c "kafka-console-consumer --bootstrap-server localhost:9092 --topic bookings --from-beginning"
```

Exit with Ctrl+C.

---

## Shutdown procedure

Stop application processes first using Ctrl+C in each terminal.

Then stop containers:

```bash
docker compose down
```

---

## Known non issues in Iteration 2

* Redis is running but not used
* Promotions are limited to one per flight by design
* No machine learning is present
* The system is single node and non fault tolerant
* This is intentional for Iteration 2

---

## Purpose of this runbook

This runbook exists so that:

* anyone can run the system consistently
* automated agents can validate behaviour
* reviewers can understand system orchestration
* Iteration 2 is reproducible and defensible
