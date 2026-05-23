from datetime import datetime
from utils.logger import get_logger

logger = get_logger("data_processor")
logger.info("🧹 Data processor initialized.")

def clean_weather_data(raw_data: dict) -> dict:
    """
    Extract and clean relevant fields from raw OpenWeather API response.
    Now handles country-level data.
    """
    if not raw_data or "main" not in raw_data:
        logger.warning("❌ Invalid raw data received")
        return {}

    try:
        cleaned = {
            "city": raw_data.get("name", "Unknown"),
            "country_code": raw_data.get("country_code", "Unknown"),  # From producer
            "temperature": round(raw_data.get("main", {}).get("temp", 0.0), 2),
            "feels_like": round(raw_data.get("main", {}).get("feels_like", 0.0), 2),
            "description": raw_data.get("weather", [{}])[0].get("description", "N/A"),
            "humidity": raw_data.get("main", {}).get("humidity", 0),
            "pressure": raw_data.get("main", {}).get("pressure", 0),
            "wind_speed": raw_data.get("wind", {}).get("speed", 0.0),
            "wind_deg": raw_data.get("wind", {}).get("deg", 0),
            "visibility": raw_data.get("visibility", 0),
            "clouds": raw_data.get("clouds", {}).get("all", 0),
            "timestamp": datetime.utcnow().isoformat(),
            "kafka_timestamp": raw_data.get('kafka_timestamp'),
            "data_type": raw_data.get('data_type', 'weather')
        }
        
        logger.debug(f"🧹 Cleaned data for {cleaned['city']}, {cleaned['country_code']}")
        return cleaned
        
    except Exception as e:
        logger.error(f"❌ Error cleaning weather data: {e}")
        return {}

def validate_weather_data(data: dict) -> bool:
    """
    Validate that all critical fields exist and have valid data types.
    """
    required_fields = ["city", "country_code", "temperature", "description", "humidity", "wind_speed"]
    
    for field in required_fields:
        if field not in data or data[field] is None:
            logger.warning(f"⚠️ Missing or invalid field: {field}")
            return False
            
    # Additional validation
    if not isinstance(data.get("temperature"), (int, float)):
        logger.warning("⚠️ Invalid temperature type")
        return False
        
    if data.get("humidity") < 0 or data.get("humidity") > 100:
        logger.warning("⚠️ Humidity out of range")
        return False
        
    return True

def process_weather_record(raw_data: dict) -> dict:
    """
    Combine cleaning and validation steps.
    Returns a processed weather record or None if invalid.
    """
    cleaned = clean_weather_data(raw_data)
    if validate_weather_data(cleaned):
        logger.info(f"✅ Successfully processed weather data for {cleaned['city']}, {cleaned['country_code']}")
        return cleaned
    else:
        logger.warning("❌ Invalid data skipped after validation")
        return None