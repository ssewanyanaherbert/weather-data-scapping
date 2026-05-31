import os
from dotenv import load_dotenv

load_dotenv()

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")

# ── Location ──────────────────────────────────────────────────────────────────
# Primary city (used as first entry if CITIES is not set)
CITY = os.getenv("CITY", "Kampala")
COUNTRY = os.getenv("COUNTRY", "UG")

# Optional: comma-separated list of extra cities in .env
# Format: EXTRA_CITIES=Entebbe,UG;Gulu,UG;Mbarara,UG
# If not set, only CITY,COUNTRY is used
_extra = os.getenv("EXTRA_CITIES", "")
EXTRA_CITIES: list[str] = [c.strip() for c in _extra.split(";") if c.strip()]

# ── OpenWeather API ───────────────────────────────────────────────────────────
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")

if not OPENWEATHER_API_KEY:
    raise ValueError(
        "❌ OPENWEATHER_API_KEY is not set. "
        "Add it to your .env file: OPENWEATHER_API_KEY=your_key_here"
    )

# ── Database ──────────────────────────────────────────────────────────────────
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "weatherdb")