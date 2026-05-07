## Project Overview

This project implements a complete data engineering workflow for a ride-hailing platform, simulating how companies like Uber might build their analytical infrastructure. It ingests raw travel data incrementally, processes real-time trip events via Spark Streaming, transforms data using dbt, and produces data for gold layer which is stored in databricks.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Batch Processing | Apache Spark (PySpark) |
| Stream Processing | Spark Structured Streaming |
| Transformation & Modeling | dbt (Data Build Tool) |
| Data Modeling | Star Schema |
| SCD Tracking | dbt Snapshots (Type 2) |
| Language | Python, SQL |

---

### Dimension Tables

| Table | Description |
|---|---|
| `drivers` | Driver profiles — name, vehicle id, city, rating, contact |
| `customers` | Customer profiles — name, contact, sign-up date, domain |
| `vehicles` | Vehicle details — make, model, vehicle type, license plate |
| `locations` | latitude, longitude, address |
| `payments` | payment method, payment status, amount |

### Fact Table

| Table | Description |
|---|---|
| `trips` | Central fact table — trip events linked to all dimensions via foreign keys, including distance, duration, fare, and timestamps |

### Entity Relationship Overview

```
drivers   ──┐
customers ──┤
vehicles  ──┼──► trips ◄── payments
locations ──┘
```
---

## Key Features

- **Incremental ingestion**: only new or updated records are processed on each run, avoiding full reloads
- **Spark Streaming**: real-time processing of trip events as they arrive
- **dbt staging models**: standardise raw data (dedup, naming conventions) before transformation
- **dbt snapshots**: automatically track historical changes to dimension records using SCD Type 2

---

## What I learnt

1. dbt fundamentals
2. ref() and source() - DAG-based dependency management
3. Snapshots & SCD Type 2 — tracking historical changes to dimension records
4. Jinja templating
