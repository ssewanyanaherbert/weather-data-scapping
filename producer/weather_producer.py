import requests
from utils.kafka_client import create_kafka_producer
from config import settings
import time
from utils.logger import get_logger

logger = get_logger("weather_producer")
logger.info("🌤️ Weather producer initialized.")


# ── Cities to scrape ──────────────────────────────────────────────────────────
# Add or remove cities freely — no lat/lon needed.
# Format: "CityName,CountryCode"
# These can also be moved to .env as CITIES=Kampala,UG;Entebbe,UG;Gulu,UG
CITIES = [
    f"{settings.CITY},{settings.COUNTRY}",   # primary city from .env (e.g. Kampala,UG)
    f"Entebbe,{settings.COUNTRY}",
    f"Gulu,{settings.COUNTRY}",
    f"Mbarara,{settings.COUNTRY}",
    f"Jinja,{settings.COUNTRY}",
    f"Mbale,{settings.COUNTRY}",
    f"Masaka,{settings.COUNTRY}",
    f"Lira,{settings.COUNTRY}",
]


def wait_for_kafka(max_retries=30, retry_interval=5):
    """Wait for Kafka to become available."""
    for i in range(max_retries):
        try:
            producer = create_kafka_producer()
            producer.list_topics(timeout=10)
            producer.close()
            logger.info("✅ Kafka is available!")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Kafka not available (attempt {i+1}/{max_retries}): {e}")
            if i < max_retries - 1:
                time.sleep(retry_interval)

    logger.error(f"❌ Kafka not available after {max_retries} attempts")
    return False


def geocode_city(city_query: str) -> dict | None:
    """
    Use the OpenWeather Geocoding API to resolve a city name to lat/lon.
    This is FREE and does not count against your weather API quota.

    Args:
        city_query: e.g. "Kampala,UG"

    Returns:
        dict with lat, lon, name, country — or None on failure
    """
    url = (
        f"http://api.openweathermap.org/geo/1.0/direct"
        f"?q={city_query}"
        f"&limit=1"
        f"&appid={settings.OPENWEATHER_API_KEY}"
    )

    try:
        response = requests.get(url, timeout=10)
        results = response.json()

        if results and isinstance(results, list):
            location = results[0]
            logger.info(
                f"📍 Geocoded '{city_query}' → "
                f"lat={location['lat']}, lon={location['lon']}, "
                f"name={location.get('name')}"
            )
            return {
                "lat": location["lat"],
                "lon": location["lon"],
                "name": location.get("name", city_query),
                "country": location.get("country", settings.COUNTRY),
            }
        else:
            logger.warning(f"⚠️ Geocoding returned no results for '{city_query}'")
            return None

    except requests.exceptions.RequestException as e:
        logger.warning(f"⚠️ Geocoding request failed for '{city_query}': {e}")
        return None


def fetch_weather_by_coords(lat: float, lon: float) -> dict | None:
    """
    Fetch weather data from OpenWeather using lat/lon.
    Coordinates are resolved automatically via geocode_city() —
    you never need to hardcode them.
    """
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}"
        f"&appid={settings.OPENWEATHER_API_KEY}"
        f"&units=metric"
    )

    try:
        response = requests.get(url, timeout=10)
        data = response.json()

        if data.get("cod") == 200:
            return data
        else:
            logger.warning(
                f"⚠️ Weather API returned cod={data.get('cod')}: {data.get('message')}"
            )
            return None

    except requests.exceptions.RequestException as e:
        logger.warning(f"⚠️ Weather request failed: {e}")
        return None


def fetch_weather_by_name(city_query: str) -> dict | None:
    """
    Fetch weather for a city by name without any hardcoded coordinates.

    Flow:
        1. Geocoding API resolves city name → lat/lon  (free, automatic)
        2. Weather API fetches data using those coords
        3. Falls back to ?q= query if geocoding fails

    Args:
        city_query: e.g. "Kampala,UG"

    Returns:
        Enriched weather dict or None
    """
    # Step 1: Resolve city name to coordinates
    location = geocode_city(city_query)

    if location:
        # Step 2: Fetch weather using resolved coordinates
        data = fetch_weather_by_coords(location["lat"], location["lon"])
    else:
        # Fallback: use ?q= directly (less precise but still works)
        logger.warning(f"⚠️ Geocoding failed for '{city_query}', falling back to ?q= query")
        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?q={city_query}"
            f"&appid={settings.OPENWEATHER_API_KEY}"
            f"&units=metric"
        )
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            if data.get("cod") != 200:
                logger.error(
                    f"❌ Fallback also failed for '{city_query}': {data.get('message')}"
                )
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Fallback request failed for '{city_query}': {e}")
            return None

    if data:
        # Enrich with pipeline metadata
        data["country_code"] = (location or {}).get("country", settings.COUNTRY)
        data["kafka_timestamp"] = time.time()
        data["data_type"] = "country_weather"

    return data


def produce_weather_data(interval=300):
    """
    Continuously fetch weather for ALL configured cities and publish to Kafka.
    Each city is fetched and published in sequence per cycle.
    No lat/lon is ever hardcoded — all coordinates are resolved automatically.
    """
    if not wait_for_kafka():
        logger.error("❌ Exiting: Kafka is not available")
        return

    producer = create_kafka_producer()

    logger.info(f"🚀 Starting weather producer for {len(CITIES)} cities:")
    for city in CITIES:
        logger.info(f"   🏙️  {city}")
    logger.info(f"📡 Fetch interval: every {interval} seconds")

    while True:
        successful = 0
        failed = 0

        for city_query in CITIES:
            try:
                data = fetch_weather_by_name(city_query)

                if data and data.get("cod") == 200:
                    future = producer.send(settings.WEATHER_TOPIC, value=data)
                    future.get(timeout=10)

                    city_name  = data.get("name", city_query)
                    temperature = data.get("main", {}).get("temp", "N/A")
                    description = data.get("weather", [{}])[0].get("description", "N/A")
                    country     = data.get("country_code", settings.COUNTRY)

                    logger.info(
                        f"✅ {city_name}, {country} | "
                        f"🌡️ {temperature}°C | ☁️ {description}"
                    )
                    successful += 1
                else:
                    logger.warning(f"⚠️ Skipped '{city_query}' — no valid data returned")
                    failed += 1

            except Exception as e:
                logger.error(f"❌ Error processing '{city_query}': {e}")
                failed += 1
                # Recreate producer on connection error
                try:
                    producer.close()
                except Exception:
                    pass
                producer = create_kafka_producer()

        producer.flush()
        logger.info(
            f"🔄 Cycle complete — ✅ {successful} sent, ❌ {failed} failed. "
            f"Next fetch in {interval}s..."
        )
        time.sleep(interval)