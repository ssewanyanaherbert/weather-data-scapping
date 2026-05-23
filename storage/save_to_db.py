from sqlalchemy import Column, Integer, String, Float, DateTime, BigInteger
from datetime import datetime
from storage.db_client import Base, engine, get_db_session
from sqlalchemy.exc import SQLAlchemyError
from utils.logger import get_logger

logger = get_logger("save_to_db")
logger.info("Save to DB module initialized.")

class WeatherRecord(Base):
    """SQLAlchemy model updated to match data_processor output."""
    __tablename__ = "weather_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # City and location info - UPDATED to match data_processor
    city = Column(String(255))  # Changed from city_name
    country_code = Column(String(10))  # Changed from country
    
    # Weather conditions - UPDATED to match data_processor
    temperature = Column(Float)  # Changed from temp
    feels_like = Column(Float)
    description = Column(String(255))  # Changed from weather_description
    humidity = Column(Integer)
    pressure = Column(Integer)
    wind_speed = Column(Float)
    wind_deg = Column(Integer, nullable=True)
    
    # Additional fields from data_processor
    visibility = Column(Integer)
    clouds = Column(Integer)  # Changed from clouds_all
    
    # Timestamps
    recorded_at = Column(DateTime, default=datetime.utcnow)
    kafka_timestamp = Column(Float, nullable=True)  # New field
    data_type = Column(String(50), nullable=True)  # New field
    
    # Optional fields (if available in raw data)
    longitude = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    weather_main = Column(String(50), nullable=True)
    weather_icon = Column(String(10), nullable=True)

# Create the table if it doesn't exist
Base.metadata.create_all(engine)

def save_weather_record(processed_record: dict):
    """
    Save a processed weather record from data_processor to PostgreSQL.
    
    Args:
        processed_record: Dictionary from data_processor.clean_weather_data()
    """
    session = get_db_session()
    try:
        # Create WeatherRecord from processed data (matches data_processor output)
        weather = WeatherRecord(
            # City and location
            city=processed_record.get("city", "Unknown"),
            country_code=processed_record.get("country_code", "Unknown"),
            
            # Weather conditions
            temperature=processed_record.get("temperature"),
            feels_like=processed_record.get("feels_like"),
            description=processed_record.get("description"),
            humidity=processed_record.get("humidity"),
            pressure=processed_record.get("pressure"),
            wind_speed=processed_record.get("wind_speed"),
            wind_deg=processed_record.get("wind_deg"),
            
            # Additional fields
            visibility=processed_record.get("visibility"),
            clouds=processed_record.get("clouds"),
            
            # Timestamps
            recorded_at=datetime.utcnow(),
            kafka_timestamp=processed_record.get("kafka_timestamp"),
            data_type=processed_record.get("data_type", "weather")
        )
        
        session.add(weather)
        session.commit()
        logger.info(f"✅ Saved processed weather data for {weather.city}, {weather.country_code}")
        return weather.id
        
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"❌ Database error: {e}")
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Unexpected error: {e}")
        raise
    finally:
        session.close()