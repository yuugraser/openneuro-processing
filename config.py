"""
Configuration settings for the OpenNeuro Desktop Application.
"""
from pathlib import Path
import json

# Base paths
BASE_DIR = Path(__file__).parent
TEMP_DIR = BASE_DIR / "temp"
CACHE_DIR = BASE_DIR / "cache"
CONFIG_FILE = BASE_DIR / "user_config.json"

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

def load_user_config():
    """Load user configuration from file."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_user_config(config):
    """Save user configuration to file."""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f)

def create_directories():
    """Create necessary application directories."""
    TEMP_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_ok=True)

def cleanup_temp():
    """Clean up temporary files."""
    import shutil
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    TEMP_DIR.mkdir(exist_ok=True)