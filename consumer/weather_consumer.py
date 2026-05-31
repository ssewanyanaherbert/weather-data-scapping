from utils.kafka_client import create_kafka_consumer
from config import settings
from consumer.data_processor import process_weather_record
from storage.save_to_db import save_weather_record
from utils.logger import get_logger
import time

logger = get_logger("weather_consumer")
logger.info("🎧 Weather consumer initialized.")


def wait_for_kafka_and_topic(max_retries=30, retry_interval=5):
    """Wait for Kafka and the weather topic to become available."""
    for i in range(max_retries):
        try:
            consumer = create_kafka_consumer()
            topics = consumer.topics()

            if settings.WEATHER_TOPIC in topics:
                consumer.close()
                logger.info(f"✅ Kafka and topic '{settings.WEATHER_TOPIC}' are available!")
                return True
            else:
                logger.warning(
                    f"⚠️ Kafka available but topic '{settings.WEATHER_TOPIC}' "
                    f"not found (attempt {i+1}/{max_retries})"
                )
                consumer.close()

        except Exception as e:
            logger.warning(f"⚠️ Kafka not available (attempt {i+1}/{max_retries}): {e}")

        if i < max_retries - 1:
            time.sleep(retry_interval)

    logger.error(f"❌ Kafka/topic not available after {max_retries} attempts")
    return False


def consume_weather_data():
    """Consume, process, and save weather data from Kafka."""
    if not wait_for_kafka_and_topic():
        logger.error("❌ Exiting: Kafka or topic is not available")
        return

    consumer = create_kafka_consumer()
    logger.info(f"📡 Listening to topic: {settings.WEATHER_TOPIC}")
    logger.info("✅ Consumer ready and waiting for messages...")

    try:
        for message in consumer:
            try:
                raw_record = message.value

                city = raw_record.get("name", "Unknown")
                country = raw_record.get("country_code", settings.COUNTRY)
                logger.info(f"📥 Received weather data for {city}, {country}")

                processed = process_weather_record(raw_record)

                if processed:
                    save_weather_record(processed)
                    logger.info(
                        f"💾 Saved: {processed.get('city', 'Unknown')} | "
                        f"🌡️ {processed.get('temperature')}°C | "
                        f"☁️ {processed.get('description')}"
                    )
                else:
                    logger.warning("⚠️ Skipped: record failed validation")

            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
    except Exception as e:
        logger.error(f"❌ Unexpected consumer error: {e}")
    finally:
        consumer.close()
        logger.info("🧹 Consumer closed cleanly")