# Weather Data Pipeline

A real-time weather data pipeline that fetches live weather data from the OpenWeather API, streams it through Apache Kafka, processes and validates it, then persists it to a PostgreSQL database.

---

## Architecture Overview

```
OpenWeather API
      │
      ▼
 weather_producer.py   ←── scheduler.py (every 10 min)
      │
      │  (Kafka topic: weather_data)
      ▼
 weather_consumer.py
      │
      ▼
 data_processor.py  (clean + validate)
      │
      ▼
 save_to_db.py  →  PostgreSQL (weather_data table)
```

---

## Project Structure

```
clickhouse/
├── config/
│   ├── __init__.py
│   └── settings.py              # Environment config loader
├── consumer/
│   ├── __init__.py
│   ├── data_processor.py        # Cleans and validates raw API data
│   └── weather_consumer.py      # Kafka consumer loop
├── docker/
│   ├── docker-compose.yml       # Kafka + PostgreSQL services
│   └── Dockerfile
├── logs/                        # Auto-generated log files
├── producer/
│   ├── __init__.py
│   ├── scheduler.py             # Runs producer on a schedule
│   └── weather_producer.py      # Fetches from API, publishes to Kafka
├── storage/
│   ├── db_client.py             # SQLAlchemy engine + session factory
│   └── save_to_db.py            # ORM model + DB write logic
├── utils/
│   ├── __init__.py
│   ├── kafka_client.py          # Producer/consumer factory with retry
│   └── logger.py                # Shared logger
├── .env                         # Environment variables (not committed)
├── main.py
├── requirements.txt
└── README.md
```

---

## Prerequisites

- Python 3.10+
- Docker and Docker Compose
- An [OpenWeather API key](https://home.openweathermap.org/api_keys) (free tier works)

---

## Setup

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd clickhouse
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate        # Linux / macOS
venv\Scripts\activate           # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root:

```dotenv
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
WEATHER_TOPIC=weather_data

# Location (used to query OpenWeather — no lat/lon needed)
CITY=Kampala
COUNTRY=UG

# OpenWeather API
OPENWEATHER_API_KEY=your_api_key_here

# PostgreSQL
DB_USER=
DB_PASSWORD=
DB_HOST=
DB_PORT=
DB_NAME=
```

> **Note:** New OpenWeather API keys can take up to 2 hours to activate after registration.

### 5. Start Kafka and PostgreSQL via Docker

```bash
docker-compose -f docker/docker-compose.yml up -d
```

---

## Running the Pipeline

### Start the producer (continuous, every 5 minutes)

```bash
bash scripts/run_producer.sh
# or directly:
python -m producer.weather_producer
```

### Start the scheduler (every 10 minutes, runs immediately on start)

```bash
python -m producer.scheduler
```

### Start the consumer

```bash
bash scripts/run_consumer.sh
# or directly:
python -m consumer.weather_consumer
```

---

## How It Works

### Producer (`weather_producer.py`)

Queries the OpenWeather `/data/2.5/weather` endpoint using `CITY,COUNTRY` from your `.env` (e.g. `Kampala,UG`). No latitude or longitude is required. It tries three query formats in priority order:

1. `Kampala,UG` — most reliable
2. `Kampala` — fallback
3. `,UG` — last resort

On success the raw API response is enriched with pipeline metadata (`country_code`, `kafka_timestamp`, `data_type`) and published to the Kafka topic.

### Consumer (`weather_consumer.py`)

Reads messages from the Kafka topic, passes each one through the data processor, and writes the result to PostgreSQL.

### Data Processor (`data_processor.py`)

Extracts and maps every field from the OpenWeather response:

| Category | Fields captured |
|---|---|
| Location | `city`, `country_code`, `longitude`, `latitude` |
| Condition | `weather_id`, `weather_main`, `description`, `weather_icon` |
| Temperature | `temperature`, `feels_like`, `temp_min`, `temp_max` |
| Atmosphere | `pressure`, `humidity`, `sea_level`, `grnd_level` |
| Wind | `wind_speed`, `wind_deg`, `wind_gust` |
| Other | `visibility`, `clouds`, `base` |
| OW timestamps | `dt`, `timezone`, `sunrise`, `sunset` |
| Pipeline | `timestamp`, `kafka_timestamp`, `data_type` |

Temperatures are in **°C** (the API is called with `units=metric`). Validation rejects any record missing `city`, `country_code`, `temperature`, `description`, `humidity`, or `wind_speed`.

### Database (`save_to_db.py`)

All processed fields are persisted to a `weather_data` table in PostgreSQL. The table is created automatically on first run via `Base.metadata.create_all(engine)`.

---

## Database Schema

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `id` | INTEGER | No | Auto-increment PK |
| `city` | VARCHAR(255) | No | |
| `country_code` | VARCHAR(10) | No | |
| `longitude` | FLOAT | Yes | |
| `latitude` | FLOAT | Yes | |
| `weather_id` | INTEGER | Yes | OW condition code |
| `weather_main` | VARCHAR(50) | Yes | e.g. "Clear" |
| `description` | VARCHAR(255) | No | e.g. "clear sky" |
| `weather_icon` | VARCHAR(10) | Yes | e.g. "01d" |
| `temperature` | FLOAT | No | °C |
| `feels_like` | FLOAT | Yes | °C |
| `temp_min` | FLOAT | Yes | °C |
| `temp_max` | FLOAT | Yes | °C |
| `pressure` | INTEGER | Yes | hPa |
| `humidity` | INTEGER | No | % |
| `sea_level` | INTEGER | Yes | hPa |
| `grnd_level` | INTEGER | Yes | hPa |
| `wind_speed` | FLOAT | Yes | m/s |
| `wind_deg` | INTEGER | Yes | degrees |
| `wind_gust` | FLOAT | Yes | m/s |
| `visibility` | INTEGER | Yes | metres |
| `clouds` | INTEGER | Yes | % coverage |
| `base` | VARCHAR(50) | Yes | |
| `dt` | BIGINT | Yes | Unix UTC timestamp |
| `timezone` | INTEGER | Yes | Seconds offset from UTC |
| `sunrise` | BIGINT | Yes | Unix UTC |
| `sunset` | BIGINT | Yes | Unix UTC |
| `recorded_at` | DATETIME | No | Pipeline insert time |
| `kafka_timestamp` | FLOAT | Yes | Time message was produced |
| `data_type` | VARCHAR(50) | Yes | Always `"country_weather"` |

---

## Logs

Log files are written to the `logs/` directory:

| File | Component |
|---|---|
| `weather_producer.log` | API fetch and Kafka publish |
| `weather_scheduler.log` | Scheduler job runs |
| `weather_consumer.log` | Kafka consume loop |
| `data_processor.log` | Clean and validate steps |
| `save_to_db.log` | DB writes |
| `kafka_client.log` | Producer/consumer creation |
| `db_client.log` | DB connection events |

---

## Troubleshooting

**401 from OpenWeather API**
- Check that `OPENWEATHER_API_KEY` is set correctly in `.env` with no extra spaces.
- New keys take up to 2 hours to activate — wait and retry.

**Kafka not available**
- Confirm Docker containers are running: `docker ps`
- Check `KAFKA_BOOTSTRAP_SERVERS` matches the port exposed in `docker-compose.yml`.

**Database connection failed**
- Confirm PostgreSQL is running and `DB_*` variables in `.env` are correct.
- The app will raise immediately at startup if the DB is unreachable.

**Temperatures look wrong (very high numbers)**
- The API URL must include `&units=metric`. Check `weather_producer.py` — this is set by default.

---

## Requirements

Key packages (see `requirements.txt` for pinned versions):

```
kafka-python
requests
sqlalchemy
psycopg2-binary
python-dotenv
schedule
```