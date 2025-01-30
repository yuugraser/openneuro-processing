"""
Configuration settings for the OpenNeuro Desktop Application.
Handles environment variables and application settings.
"""
import errno
import os
import stat
import shutil
from pathlib import Path
from typing import Dict, Any

# Base paths
BASE_DIR = Path(__file__).parent
TEMP_DIR = BASE_DIR / "temp"
CACHE_DIR = BASE_DIR / "cache"

# AWS Configuration
AWS_REGION = "us-east-1"
S3_BUCKET = "openneuro-datasets"

# API Configuration
OPENNEURO_API_URL = "https://openneuro.org/crn"
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

def handle_remove_readonly(func, path, exc):
    """
    Handle permission errors during file deletion.

    Args:
        func: The function that failed
        path: The path of the file
        exc: The exception that was raised
    """
    excvalue = exc[1]
    if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == errno.EACCES:
        # Change the file to be readable, writable, and executable
        os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        # Try again
        func(path)
    else:
        raise

def create_directories() -> None:
    """Create necessary application directories if they don't exist."""
    TEMP_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_ok=True)

def cleanup_temp() -> None:
    """
    Clean up temporary files with proper permission handling.
    """
    if TEMP_DIR.exists():
        try:
            # First try to make all files writable
            for root, dirs, files in os.walk(TEMP_DIR, topdown=False):
                for name in files:
                    file_path = os.path.join(root, name)
                    try:
                        os.chmod(file_path, stat.S_IRWXU)
                    except OSError:
                        pass
                for name in dirs:
                    dir_path = os.path.join(root, name)
                    try:
                        os.chmod(dir_path, stat.S_IRWXU)
                    except OSError:
                        pass

            # Then remove the directory tree
            shutil.rmtree(TEMP_DIR, onerror=handle_remove_readonly)
        except Exception as e:
            import logging
            logging.warning(f"Failed to cleanup temp directory: {str(e)}")
            # If cleanup fails, we'll try again next time
            return

    # Create a new temp directory
    TEMP_DIR.mkdir(exist_ok=True)