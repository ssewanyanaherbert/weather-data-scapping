from sqlalchemy import Column, Integer, String, Float, DateTime, BigInteger
from datetime import datetime, timezone
from storage.db_client import Base, engine, get_db_session
from sqlalchemy.exc import SQLAlchemyError
from utils.logger import get_logger

logger = get_logger("save_to_db")
logger.info("💾 Save to DB module initialized.")


class WeatherRecord(Base):
    """
    SQLAlchemy model for weather_data table.
    All fields align with data_processor.clean_weather_data() output,
    which in turn maps the full OpenWeather /data/2.5/weather response.
    """
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # --- Location ---
    city         = Column(String(255))
    country_code = Column(String(10))
    longitude    = Column(Float,   nullable=True)
    latitude     = Column(Float,   nullable=True)

    # --- Weather condition ---
    weather_id   = Column(Integer, nullable=True)   # e.g. 800
    weather_main = Column(String(50),  nullable=True)   # e.g. "Clear"
    description  = Column(String(255))                  # e.g. "clear sky"
    weather_icon = Column(String(10),  nullable=True)   # e.g. "01d"

    # --- Temperature (°C) ---
    temperature = Column(Float)
    feels_like  = Column(Float,   nullable=True)
    temp_min    = Column(Float,   nullable=True)
    temp_max    = Column(Float,   nullable=True)

    # --- Atmosphere ---
    pressure    = Column(Integer, nullable=True)
    humidity    = Column(Integer)
    sea_level   = Column(Integer, nullable=True)   # not always in API response
    grnd_level  = Column(Integer, nullable=True)   # not always in API response

    # --- Wind ---
    wind_speed  = Column(Float,   nullable=True)
    wind_deg    = Column(Integer, nullable=True)
    wind_gust   = Column(Float,   nullable=True)   # not always in API response

    # --- Other atmospheric ---
    visibility  = Column(Integer, nullable=True)
    clouds      = Column(Integer, nullable=True)
    base        = Column(String(50), nullable=True)

    # --- OpenWeather timestamps ---
    dt          = Column(BigInteger, nullable=True)   # Unix UTC, data calc time
    timezone    = Column(Integer,    nullable=True)   # Offset in seconds from UTC
    sunrise     = Column(BigInteger, nullable=True)
    sunset      = Column(BigInteger, nullable=True)

    # --- Pipeline timestamps & metadata ---
    recorded_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    kafka_timestamp = Column(Float,      nullable=True)
    data_type       = Column(String(50), nullable=True)


# Create table if it doesn't already exist
Base.metadata.create_all(engine)


def save_weather_record(processed_record: dict):
    """
    Persist a fully processed weather record to PostgreSQL.

    Args:
        processed_record: dict returned by data_processor.process_weather_record()

    Returns:
        int: primary-key id of the newly inserted row

    Raises:
        SQLAlchemyError: on database errors (after rollback)
        Exception:       on any other unexpected error (after rollback)
    """
    session = get_db_session()
    try:
        weather = WeatherRecord(
            # Location
            city=processed_record.get("city", "Unknown"),
            country_code=processed_record.get("country_code", "Unknown"),
            longitude=processed_record.get("longitude"),
            latitude=processed_record.get("latitude"),

            # Weather condition
            weather_id=processed_record.get("weather_id"),
            weather_main=processed_record.get("weather_main"),
            description=processed_record.get("description"),
            weather_icon=processed_record.get("weather_icon"),

            # Temperature
            temperature=processed_record.get("temperature"),
            feels_like=processed_record.get("feels_like"),
            temp_min=processed_record.get("temp_min"),
            temp_max=processed_record.get("temp_max"),

            # Atmosphere
            pressure=processed_record.get("pressure"),
            humidity=processed_record.get("humidity"),
            sea_level=processed_record.get("sea_level"),
            grnd_level=processed_record.get("grnd_level"),

            # Wind
            wind_speed=processed_record.get("wind_speed"),
            wind_deg=processed_record.get("wind_deg"),
            wind_gust=processed_record.get("wind_gust"),

            # Other atmospheric
            visibility=processed_record.get("visibility"),
            clouds=processed_record.get("clouds"),
            base=processed_record.get("base"),

            # OpenWeather timestamps
            dt=processed_record.get("dt"),
            timezone=processed_record.get("timezone"),
            sunrise=processed_record.get("sunrise"),
            sunset=processed_record.get("sunset"),

            # Pipeline metadata
            recorded_at=datetime.now(timezone.utc),
            kafka_timestamp=processed_record.get("kafka_timestamp"),
            data_type=processed_record.get("data_type", "country_weather"),
        )

        session.add(weather)
        session.commit()

        logger.info(
            f"✅ Saved row id={weather.id} | "
            f"{weather.city}, {weather.country_code} | "
            f"🌡️ {weather.temperature}°C | ☁️ {weather.description}"
        )
        return weather.id

    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"❌ Database error saving weather record: {e}")
        raise

    except Exception as e:
        session.rollback()
        logger.error(f"❌ Unexpected error saving weather record: {e}")
        raise

    finally:
        session.close()