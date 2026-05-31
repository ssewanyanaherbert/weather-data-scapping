import schedule
import time
from producer.weather_producer import fetch_country_weather_data
from utils.kafka_client import create_kafka_producer
from config import settings
from utils.logger import get_logger

logger = get_logger("weather_scheduler")
logger.info("📅 Weather scheduler initialized.")


def scheduled_job():
    """Scheduled job to fetch and send weather data to Kafka."""
    producer = None
    try:
        producer = create_kafka_producer()

        data = fetch_country_weather_data(settings.COUNTRY)

        if data and data.get("cod") == 200:
            future = producer.send(settings.WEATHER_TOPIC, value=data)
            future.get(timeout=10)

            city_name = data.get("name", "Unknown location")
            temperature = data.get("main", {}).get("temp", "N/A")
            description = data.get("weather", [{}])[0].get("description", "N/A")

            logger.info(
                f"✅ Scheduled job: Sent data for {city_name} | "
                f"🌡️ {temperature}°C | ☁️ {description}"
            )
        else:
            error_msg = data.get("message", "Unknown error") if data else "No data received"
            logger.warning(
                f"⚠️ Scheduled job: Failed to fetch data for "
                f"{settings.CITY},{settings.COUNTRY}: {error_msg}"
            )

    except Exception as e:
        logger.error(f"❌ Scheduled job failed: {e}")

    finally:
        # Always close the producer cleanly
        if producer:
            try:
                producer.flush()
                producer.close()
            except Exception as e:
                logger.warning(f"⚠️ Could not close producer cleanly: {e}")


def start_scheduler(interval_minutes=10):
    """Start the weather data scheduler."""
    logger.info(
        f"🚀 Starting scheduler for {settings.CITY},{settings.COUNTRY} "
        f"— every {interval_minutes} minute(s)"
    )

    # Run immediately on startup
    scheduled_job()

    # Then schedule recurring runs
    schedule.every(interval_minutes).minutes.do(scheduled_job)

    logger.info(f"🕒 Scheduler running: every {interval_minutes} minutes")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Scheduler stopped by user")