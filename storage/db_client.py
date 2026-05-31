from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from utils.logger import get_logger
import os

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


def test_db_connection():
    """
    Verify the database is reachable.
    Raises immediately if the connection fails so the app doesn't start silently broken.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection successful")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise


def get_db_session():
    """Return a new SQLAlchemy session."""
    return SessionLocal()


# ---------------------------------------------------------------------------
# Module-level engine and session factory
# ---------------------------------------------------------------------------
DATABASE_URL = get_database_url()

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,       # drops stale connections before use
    pool_size=5,              # number of persistent connections
    max_overflow=10,          # extra connections allowed under load
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

# Test connectivity at startup so misconfiguration is caught early
test_db_connection()