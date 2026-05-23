from kafka import KafkaProducer, KafkaConsumer
import json
from config import settings
from utils.logger import get_logger
import time

logger = get_logger("kafka_client")
logger.info("🎡 Kafka client initialized.")

def create_kafka_producer(max_retries=3, retry_delay=2):
    """Create and return a Kafka producer with retry logic."""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                request_timeout_ms=15000,
                api_version_auto_timeout_ms=30000
            )
            # Test the connection
            producer.list_topics(timeout=10)
            logger.info(f"✅ Kafka producer created successfully")
            return producer
        except Exception as e:
            logger.warning(f"⚠️ Failed to create Kafka producer (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ Failed to create Kafka producer after {max_retries} attempts")
                raise

def create_kafka_consumer(topic=settings.WEATHER_TOPIC, max_retries=10, retry_delay=5):
    """Create and return a Kafka consumer with retry logic."""
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="weather-consumer-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            # Test the connection by getting consumer assignment (non-blocking)
            consumer.topics()
            logger.info(f"✅ Kafka consumer created successfully for topic: {topic}")
            return consumer
        except Exception as e:
            logger.warning(f"⚠️ Failed to create Kafka consumer (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ Failed to create Kafka consumer after {max_retries} attempts")
                raise