"""
Entry point for the OpenNeuro Desktop Application.
Handles initialization, logging setup, and application launch.
"""
import sys
import logging
import subprocess
from pathlib import Path
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt
from datetime import datetime
from gui import MainWindow
from config import BASE_DIR, create_directories, cleanup_temp
from packaging import version

def setup_logging() -> None:
    """Configure application logging."""
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"openneuro_app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def check_git_annex() -> None:
    """Check if git-annex is installed and meets version requirements."""
    try:
        result = subprocess.run(['git-annex', 'version'],
                              capture_output=True,
                              text=True)

        if result.returncode != 0:
            raise RuntimeError("git-annex is not installed")

        version_line = [line for line in result.stdout.split('\n')
                       if line.startswith('git-annex version:')][0]
        current_version = version_line.split(':')[1].strip()

        if version.parse(current_version) < version.parse("8.20200309"):
            raise RuntimeError(
                f"git-annex version {current_version} is too old. Version >= 8.20200309 required"
            )

    except FileNotFoundError:
        raise RuntimeError("git-annex is not installed")

def check_dependencies() -> None:
    """Check required dependencies."""
    try:
        check_git_annex()
    except RuntimeError as e:
        error_msg = (
            f"git-annex error: {str(e)}\n\n"
            "Please install git-annex >= 8.20200309:\n\n"
            "macOS (with Homebrew):\n"
            "    brew install git-annex\n\n"
            "Ubuntu/Debian:\n"
            "    sudo apt-get install git-annex\n\n"
            "For more information, visit:\n"
            "http://handbook.datalad.org/r.html?install"
        )
        raise RuntimeError(error_msg)

    # Check AWS credentials
    try:
        import boto3
        boto3.client('s3')
    except Exception as e:
        raise RuntimeError(
            f"AWS credentials not found or invalid: {str(e)}\n"
            "Please configure AWS CLI first."
        )

def initialize_app() -> None:
    """Initialize application requirements."""
    try:
        cleanup_temp()
    except Exception as e:
        logging.warning(f"Failed to clean temporary directory: {str(e)}")

    create_directories()
    check_dependencies()

def main() -> None:
    """Main application entry point."""
    try:
        # Initialize logging
        setup_logging()
        logging.info("Starting OpenNeuro Desktop Application")

        # Initialize application requirements
        initialize_app()

        # Create Qt application
        app = QApplication(sys.argv)
        app.setStyle('Fusion')

        # Create and show main window
        window = MainWindow()
        window.show()

        # Start event loop
        exit_code = app.exec()

        # Cleanup on exit
        try:
            cleanup_temp()
        except Exception as e:
            logging.warning(f"Failed to cleanup on exit: {str(e)}")

        logging.info("Application closed normally")
        sys.exit(exit_code)

    except Exception as e:
        logging.critical(f"Fatal error during application startup: {str(e)}", exc_info=True)
        print(f"ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()