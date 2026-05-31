from datetime import datetime, timezone
from utils.logger import get_logger

logger = get_logger("data_processor")
logger.info("🧹 Data processor initialized.")


def clean_weather_data(raw_data: dict) -> dict:
    """
    Extract and clean ALL relevant fields from a raw OpenWeather API response.

    Expected raw shape (units=metric):
    {
        "coord":   { "lon": 32.58, "lat": 0.32 },
        "weather": [{ "id": 800, "main": "Clear", "description": "clear sky", "icon": "01d" }],
        "base":    "stations",
        "main":    { "temp": 22.5, "feels_like": 22.1, "temp_min": 21.0, "temp_max": 24.0,
                     "pressure": 1013, "humidity": 70,
                     "sea_level": 1013, "grnd_level": 960 },
        "visibility": 10000,
        "wind":    { "speed": 3.02, "deg": 66, "gust": 3.42 },
        "clouds":  { "all": 0 },
        "dt":      1779530459,
        "sys":     { "country": "UG", "sunrise": 1779507694, "sunset": 1779561857 },
        "timezone": 7200,
        "id":      233134,
        "name":    "Kampala",
        "cod":     200
    }
    """
    if not raw_data or "main" not in raw_data:
        logger.warning("❌ Invalid raw data: missing 'main' block")
        return {}

    try:
        coord   = raw_data.get("coord", {})
        main    = raw_data.get("main", {})
        wind    = raw_data.get("wind", {})
        clouds  = raw_data.get("clouds", {})
        sys_    = raw_data.get("sys", {})
        weather = raw_data.get("weather", [{}])
        w0      = weather[0] if weather else {}

        # country_code: prefer our enriched field, fall back to sys.country
        country_code = raw_data.get("country_code") or sys_.get("country", "Unknown")

        cleaned = {
            # --- Location ---
            "city":         raw_data.get("name", "Unknown"),
            "country_code": country_code,
            "longitude":    coord.get("lon"),
            "latitude":     coord.get("lat"),

            # --- Weather condition ---
            "weather_id":   w0.get("id"),
            "weather_main": w0.get("main", "N/A"),
            "description":  w0.get("description", "N/A"),
            "weather_icon": w0.get("icon"),

            # --- Temperature (degrees C, requires units=metric in API call) ---
            "temperature":  round(main.get("temp",       0.0), 2),
            "feels_like":   round(main.get("feels_like", 0.0), 2),
            "temp_min":     round(main.get("temp_min",   0.0), 2),
            "temp_max":     round(main.get("temp_max",   0.0), 2),

            # --- Atmosphere ---
            "pressure":     main.get("pressure"),
            "humidity":     main.get("humidity"),
            "sea_level":    main.get("sea_level"),   # nullable — absent for some stations
            "grnd_level":   main.get("grnd_level"),  # nullable — absent for some stations

            # --- Wind ---
            "wind_speed":   wind.get("speed", 0.0),
            "wind_deg":     wind.get("deg"),
            "wind_gust":    wind.get("gust"),        # nullable — not always reported

            # --- Other atmospheric ---
            "visibility":   raw_data.get("visibility"),
            "clouds":       clouds.get("all"),
            "base":         raw_data.get("base"),

            # --- OpenWeather timestamps ---
            "dt":           raw_data.get("dt"),       # Unix UTC, data calculation time
            "timezone":     raw_data.get("timezone"), # Offset in seconds from UTC
            "sunrise":      sys_.get("sunrise"),
            "sunset":       sys_.get("sunset"),

            # --- Pipeline metadata ---
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            "kafka_timestamp": raw_data.get("kafka_timestamp"),
            "data_type":       raw_data.get("data_type", "country_weather"),
        }

        logger.debug(f"🧹 Cleaned data for {cleaned['city']}, {cleaned['country_code']}")
        return cleaned

    except Exception as e:
        logger.error(f"❌ Error cleaning weather data: {e}")
        return {}


def validate_weather_data(data: dict) -> bool:
    """
    Validate critical fields are present and have sensible values.
    Nullable/optional fields (gust, sea_level, grnd_level, etc.) are not checked.
    """
    required_fields = [
        "city", "country_code", "temperature",
        "description", "humidity", "wind_speed",
    ]

    for field in required_fields:
        if field not in data or data[field] is None:
            logger.warning(f"⚠️ Missing or null required field: '{field}'")
            return False

    if not isinstance(data.get("temperature"), (int, float)):
        logger.warning("⚠️ Invalid type for 'temperature'")
        return False

    humidity = data.get("humidity")
    if not isinstance(humidity, (int, float)) or not (0 <= humidity <= 100):
        logger.warning(f"⚠️ Humidity out of valid range: {humidity}")
        return False

    if not isinstance(data.get("wind_speed"), (int, float)) or data["wind_speed"] < 0:
        logger.warning("⚠️ Invalid wind_speed value")
        return False

    return True


def process_weather_record(raw_data: dict) -> dict:
    """
    Clean and validate a raw OpenWeather API record.
    Returns the processed dict, or None if validation fails.
    """
    cleaned = clean_weather_data(raw_data)

    if not cleaned:
        logger.warning("❌ Cleaning produced empty result — skipping record")
        return None

    if validate_weather_data(cleaned):
        logger.info(
            f"✅ Processed: {cleaned['city']}, {cleaned['country_code']} | "
            f"🌡️ {cleaned['temperature']}°C | ☁️ {cleaned['description']}"
        )
        return cleaned

    logger.warning("❌ Validation failed — record skipped")
    return None