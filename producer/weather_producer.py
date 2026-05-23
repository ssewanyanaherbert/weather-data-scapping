import requests
from utils.kafka_client import create_kafka_producer
from config import settings
import time
from utils.logger import get_logger

logger = get_logger("weather_producer")
logger.info("🌤️ Weather producer initialized.")


def wait_for_kafka(max_retries=30, retry_interval=5):
    """Wait for Kafka to become available."""
    for i in range(max_retries):
        try:
            producer = create_kafka_producer()
            # Test connection by getting cluster metadata
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


def fetch_country_weather_data(country_code):
    """
    Fetch country-level weather data from OpenWeather API.
    Uses the country's major cities or geographic center for representative data.
    """
    # Try multiple representative locations in the country
    representative_locations = [
        f",{country_code}",  # Country-level query
    ]
    
    for location in representative_locations:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={location}&appid={settings.OPENWEATHER_API_KEY}&units=metric"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('cod') == 200:
                # Add country information to the data
                data['country_code'] = country_code
                data['kafka_timestamp'] = time.time()
                data['data_type'] = 'country_weather'
                return data
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ Failed with location {location}: {e}")
            continue
    
    logger.error(f"❌ Could not fetch weather data for country: {country_code}")
    return None


def produce_weather_data(interval=300):
    """Continuously fetch and send country-level weather data to Kafka."""
    # Wait for Kafka to be ready before creating producer
    if not wait_for_kafka():
        logger.error("❌ Exiting: Kafka is not available")
        return
    
    producer = create_kafka_producer()
    
    logger.info(f"🚀 Starting country weather producer for: {settings.COUNTRY}")
    logger.info(f"📡 Fetching country-level weather data...")

    while True:
        try:
            # Fetch country-level weather data using only the country from config
            data = fetch_country_weather_data(settings.COUNTRY)
            
            if data and data.get('cod') == 200:
                # ✅ SEND TO KAFKA
                future = producer.send(settings.WEATHER_TOPIC, value=data)
                future.get(timeout=10)  # Wait for confirmation
                
                city_name = data.get('name', 'Unknown location')
                temperature = data.get('main', {}).get('temp', 'N/A')
                
                logger.info(f"✅ Sent weather data for {settings.COUNTRY} to Kafka")
                logger.info(f"📍 Location: {city_name} | 🌡️ Temp: {temperature}°C | ☁️ {data.get('weather', [{}])[0].get('description', 'N/A')}")
                
            else:
                error_msg = data.get('message', 'Unknown error') if data else 'No data received'
                logger.warning(f"⚠️ Failed to fetch data for {settings.COUNTRY}: {error_msg}")
                
        except Exception as e:
            logger.error(f"❌ Error producing weather data: {e}")
            # Try to recreate producer if there's a connection issue
            try:
                producer.close()
            except:
                pass
            producer = create_kafka_producer()
        
        # Flush and wait
        producer.flush()
        logger.info(f"🔄 Waiting {interval} seconds before next fetch...")
        time.sleep(interval)
