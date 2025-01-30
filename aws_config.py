"""
AWS configuration dialog for the OpenNeuro Desktop Application.
"""
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel,
                             QLineEdit, QPushButton, QMessageBox)
import keyring
import json
from pathlib import Path
import boto3
from config import BASE_DIR


class AWSConfigDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("AWS Configuration")
        self.setup_ui()
        self.load_existing_config()

    def setup_ui(self):
        """Set up the dialog UI."""
        layout = QVBoxLayout(self)

        # AWS Access Key
        access_key_layout = QHBoxLayout()
        access_key_layout.addWidget(QLabel("AWS Access Key:"))
        self.access_key_input = QLineEdit()
        access_key_layout.addWidget(self.access_key_input)
        layout.addLayout(access_key_layout)

        # AWS Secret Key
        secret_key_layout = QHBoxLayout()
        secret_key_layout.addWidget(QLabel("AWS Secret Key:"))
        self.secret_key_input = QLineEdit()
        self.secret_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        secret_key_layout.addWidget(self.secret_key_input)
        layout.addLayout(secret_key_layout)

        # AWS Region
        region_layout = QHBoxLayout()
        region_layout.addWidget(QLabel("AWS Region:"))
        self.region_input = QLineEdit()
        self.region_input.setPlaceholderText("e.g., us-east-1")
        region_layout.addWidget(self.region_input)
        layout.addLayout(region_layout)

        # S3 Bucket
        bucket_layout = QHBoxLayout()
        bucket_layout.addWidget(QLabel("S3 Bucket:"))
        self.bucket_input = QLineEdit()
        bucket_layout.addWidget(self.bucket_input)
        layout.addLayout(bucket_layout)

        # Buttons
        button_layout = QHBoxLayout()
        self.test_button = QPushButton("Test Connection")
        self.test_button.clicked.connect(self.test_connection)
        button_layout.addWidget(self.test_button)

        self.save_button = QPushButton("Save")
        self.save_button.clicked.connect(self.save_config)
        button_layout.addWidget(self.save_button)

        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_button)

        layout.addLayout(button_layout)

    def load_existing_config(self):
        """Load existing AWS configuration."""
        try:
            # Load credentials from keyring
            access_key = keyring.get_password("openneuro_app", "aws_access_key")
            secret_key = keyring.get_password("openneuro_app", "aws_secret_key")

            if access_key:
                self.access_key_input.setText(access_key)
            if secret_key:
                self.secret_key_input.setText(secret_key)

            # Load non-sensitive config from file
            config_file = BASE_DIR / "aws_config.json"
            if config_file.exists():
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    self.region_input.setText(config.get('region', ''))
                    self.bucket_input.setText(config.get('bucket', ''))

        except Exception as e:
            QMessageBox.warning(self, "Warning", f"Failed to load existing configuration: {str(e)}")

    def test_connection(self):
        """Test AWS connection with provided credentials."""
        try:
            session = boto3.Session(
                aws_access_key_id=self.access_key_input.text(),
                aws_secret_access_key=self.secret_key_input.text(),
                region_name=self.region_input.text()
            )

            s3 = session.client('s3')
            s3.head_bucket(Bucket=self.bucket_input.text())

            QMessageBox.information(self, "Success", "AWS connection successful!")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"AWS connection failed: {str(e)}")

    def save_config(self):
        """Save AWS configuration."""
        try:
            # Save credentials to keyring
            keyring.set_password(
                "openneuro_app",
                "aws_access_key",
                self.access_key_input.text()
            )
            keyring.set_password(
                "openneuro_app",
                "aws_secret_key",
                self.secret_key_input.text()
            )

            # Save non-sensitive config to file
            config = {
                'region': self.region_input.text(),
                'bucket': self.bucket_input.text()
            }

            config_file = BASE_DIR / "aws_config.json"
            with open(config_file, 'w') as f:
                json.dump(config, f)

            self.accept()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save configuration: {str(e)}")

    @classmethod
    def get_aws_config(cls, parent=None):
        """
        Static method to get AWS configuration.

        Returns:
            Dictionary containing AWS configuration or None if canceled
        """
        dialog = cls(parent)
        result = dialog.exec()

        if result == QDialog.DialogCode.Accepted:
            return {
                'access_key': keyring.get_password("openneuro_app", "aws_access_key"),
                'secret_key': keyring.get_password("openneuro_app", "aws_secret_key"),
                'region': dialog.region_input.text(),
                'bucket': dialog.bucket_input.text()
            }
        return None

    @classmethod
    def get_aws_credentials(cls):
        """
        Get stored AWS credentials.

        Returns:
            Dictionary containing AWS credentials
        """
        try:
            access_key = keyring.get_password("openneuro_app", "aws_access_key")
            secret_key = keyring.get_password("openneuro_app", "aws_secret_key")

            # Load non-sensitive config
            config_file = BASE_DIR / "aws_config.json"
            if config_file.exists():
                with open(config_file, 'r') as f:
                    config = json.load(f)
            else:
                config = {}

            return {
                'access_key': access_key,
                'secret_key': secret_key,
                'region': config.get('region'),
                'bucket': config.get('bucket')
            }

        except Exception as e:
            raise RuntimeError(f"Failed to retrieve AWS credentials: {str(e)}")
