import logging
import os
import sys

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(name: str):
    """Create a configured logger."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Force UTF-8 encoding for console on Windows
    if sys.platform == 'win32':
        sys.stdout.reconfigure(encoding='utf-8')

    # File handler with UTF-8 encoding
    file_handler = logging.FileHandler(f"{LOG_DIR}/{name}.log", encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    # Log format
    formatter = logging.Formatter(
        "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Avoid duplicate handlers
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger