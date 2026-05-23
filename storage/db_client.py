
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import settings
import os
from sqlalchemy import text
from utils.logger import get_logger


logger = get_logger("db_client")
logger.info("🗄️ Database client initialized.")


Base = declarative_base()

def get_database_url():
    """Build database connection string from environment variables."""
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "weatherdb")
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

# SQLAlchemy engine and session
DATABASE_URL = get_database_url()
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)

def get_db_session():
    """Return a new SQLAlchemy session."""
    return SessionLocal()


