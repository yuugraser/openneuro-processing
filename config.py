"""
Configuration settings for the OpenNeuro Desktop Application.
"""
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent
TEMP_DIR = BASE_DIR / "temp"
CACHE_DIR = BASE_DIR / "cache"

# AWS Configuration
AWS_REGION = "us-east-1"
S3_BUCKET = "openneuro-datasets"

# Download Configuration
MAX_CONCURRENT_DOWNLOADS = 4
CHUNK_SIZE = 8192  # 8KB chunks for file handling

# Processing Configuration
SAMPLING_RATES = [100, 250, 500, 1000, 2000]  # Hz
FILTER_RANGES = {
    "delta": (0.5, 4),
    "theta": (4, 8),
    "alpha": (8, 13),
    "beta": (13, 30),
    "gamma": (30, 100)
}

def create_directories() -> None:
    """Create necessary application directories."""
    TEMP_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_ok=True)

def cleanup_temp() -> None:
    """Clean up temporary files."""
    import shutil
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    TEMP_DIR.mkdir(exist_ok=True)