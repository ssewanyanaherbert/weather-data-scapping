import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "YOUR_API_KEY")
CITY = os.getenv("CITY", "London")
COUNTRY = os.getenv("COUNTRY", "GB")
