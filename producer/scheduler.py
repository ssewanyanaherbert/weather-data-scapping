import schedule
import time
from producer.weather_producer import fetch_country_weather_data
from utils.kafka_client import create_kafka_producer
from config import settings
from utils.logger import get_logger

logger = get_logger("weather_scheduler")
logger.info("📅 Weather scheduler initialized.")

def scheduled_job():
    """Scheduled job to fetch and send weather data."""
    try:
        # Create producer for this job
        producer = create_kafka_producer()
        
        # Fetch weather data
        data = fetch_country_weather_data(settings.COUNTRY)
        
        if data and data.get('cod') == 200:
            # Send to Kafka
            future = producer.send(settings.WEATHER_TOPIC, value=data)
            future.get(timeout=10)
            
            city_name = data.get('name', 'Unknown location')
            temperature = data.get('main', {}).get('temp', 'N/A')
            
            logger.info(f"✅ Scheduled job: Sent data for {city_name} | {temperature}°C")
        else:
            logger.warning(f"⚠️ Scheduled job: No data for {settings.COUNTRY}")
        
        # Cleanup
        producer.close()
        
    except Exception as e:
        logger.error(f"❌ Scheduled job failed: {e}")

def start_scheduler(interval_minutes=10):
    """Start the weather data scheduler."""
    # Run immediately
    scheduled_job()
    
    # Schedule recurring job
    schedule.every(interval_minutes).minutes.do(scheduled_job)
    
    logger.info(f"🕒 Scheduler started: every {interval_minutes} minutes")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Scheduler stopped")
