# UK Trade Flow ETL Pipeline

A production-ready ETL pipeline that ingests monthly UK trade statistics from
the [HMRC REST API](https://api.uktradeinfo.com/OTS), cleans and normalises
the data, and loads it into a PostgreSQL data warehouse — orchestrated by
Apache Airflow and containerised with Docker.

---

## Architecture

```
api.uktradeinfo.com/OTS  (REST API, paginates at 40,000 rows via @odata.nextLink)
        │
        ▼
┌───────────────┐     ┌─────────────────┐     ┌─────────────────────┐
│   extract.py  │────▶│  transform.py   │────▶│     load.py         │
│  (requests)   │     │  (pandas)       │     │  (psycopg2 upsert)  │
└───────────────┘     └─────────────────┘     └─────────────────────┘
        │                                               │
   data/raw/*.json                             PostgreSQL trade_db
                                               └── trade_flows
                                               └── dim_countries
                                               └── dim_commodities
                                               └── dim_ports

All tasks orchestrated by:
┌─────────────────────────────────────────────────────────┐
│  Airflow DAG  uk_trade_pipeline  (monthly, 1st @ 06:00) │
│  download → validate → transform → load → log_summary   │
└─────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
uk-trade-pipeline/
├── pipeline/
│   ├── config.py       # DB config, HMRC URL, flow types
│   ├── extract.py      # Fetch from HMRC REST API (handles pagination)
│   ├── transform.py    # Clean, normalise, cast types
│   └── load.py         # Upsert into PostgreSQL
├── sql/
│   └── create_tables.sql
├── dags/
│   └── trade_pipeline_dag.py
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── uk_trade_dbt/       # dbt models (6 models, 10 tests)
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── data/raw/           # Downloaded JSON responses (git-ignored)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env                # Local secrets (git-ignored)
└── README.md
```

---

## Schema

```sql
trade_flows (
    id             BIGSERIAL PRIMARY KEY,
    period         DATE NOT NULL,          -- first day of reporting month
    commodity_id   INTEGER NOT NULL,       -- FK → dim_commodities
    flow_type      VARCHAR(50) NOT NULL,   -- EU_IMPORTS | EU_EXPORTS | NON_EU_*
    country_id     INTEGER NOT NULL,       -- FK → dim_countries
    port_id        INTEGER,               -- FK → dim_ports
    value_gbp      NUMERIC(18,2),
    net_mass_kg    NUMERIC(18,3),
    created_at     TIMESTAMPTZ DEFAULT NOW()
)

dim_countries   (263 rows)   -- country_id, country_code, country_name, trade_bloc
dim_commodities (16,243 rows) -- commodity_id, commodity_code, commodity_desc
dim_ports       (190 rows)   -- port_id, port_code, port_name
```

Unique constraint on `(period, commodity_id, flow_type, country_id, port_id)` enables
safe idempotent upserts.

---

## dbt Models

The `uk_trade_dbt/` folder contains 6 models and **10 tests — all passing**.

| Layer | Model | Materialisation | Description |
|---|---|---|---|
| Staging | `stg_trade_flows` | view | Joins all dims, adds `flow_direction` and `trade_bloc` |
| Intermediate | `int_trade_by_country` | table | Trade aggregated by country and period |
| Intermediate | `int_trade_by_commodity` | table | Trade aggregated by commodity and period |
| Marts | `mart_trade_summary` | table | High-level summary by flow type and period |
| Marts | `mart_top_imports` | table | Top importing commodities / countries |
| Marts | `mart_top_exports` | table | Top exporting commodities / countries |

```bash
cd uk_trade_dbt
dbt run    # builds all 6 models
dbt test   # runs all 10 tests
```

---

## Key Insights (January 2024)

| Metric | Value |
|---|---|
| Non-EU imports | £3.19bn |
| Non-EU exports | £963m |
| EU imports | £1.49bn |
| EU exports | £402m |
| **Total trade deficit** | **~£3.3bn** |

Post-Brexit split is clearly visible: non-EU imports dominate both in volume and
value, reflecting the shift in UK trade relationships since leaving the single market.

---

## Quick Start

### 1. Clone & configure

```bash
git clone <repo-url> && cd uk-trade-pipeline
cp .env.example .env   # edit DB_PASSWORD
```

### 2. Run with Docker Compose

```bash
docker compose up --build -d
```

This starts:
| Service | Port | Purpose |
|---|---|---|
| `postgres-trade` | **5433** | Trade data warehouse |
| `postgres-airflow` | internal | Airflow metadata |
| `airflow-webserver` | **8081** | Airflow UI (admin / admin) |
| `airflow-scheduler` | — | Triggers DAG runs |

The trade schema is applied automatically via `sql/create_tables.sql`.

### 3. Trigger a backfill manually

```bash
# Via Airflow CLI inside the scheduler container
docker compose exec airflow-scheduler \
  airflow dags trigger uk_trade_pipeline
```

Or open the Airflow UI at [http://localhost:8081](http://localhost:8081).

---

## Running Locally (without Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Ensure PostgreSQL is running on port 5433 and .env is configured
psql -h localhost -p 5433 -U daudsaid -d trade_db -f sql/create_tables.sql

# Run a single month
python - <<'EOF'
from pipeline.extract import download_period
from pipeline.transform import transform
from pipeline.load import load

filepath = download_period(2024, 1)
df = transform(filepath)
load(df)
EOF
```

---

## Running Tests

```bash
pytest tests/ -v
```

Tests use mocking and temporary directories — no live DB or network required.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5433` | PostgreSQL port |
| `DB_NAME` | `trade_db` | Database name |
| `DB_USER` | `daudsaid` | Database user |
| `DB_PASSWORD` | — | Database password |
