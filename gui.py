"""
PyQt6-based GUI for the OpenNeuro Desktop Application with background processing support.
"""
import sys
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                            QHBoxLayout, QLabel, QLineEdit, QPushButton,
                            QProgressBar, QTreeView, QMessageBox)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QStandardItemModel, QStandardItem
from pathlib import Path
from typing import List, Dict
import logging
from fetch_data import OpenNeuroAPI
from s3_upload import S3Handler
from process_ieeg import IEEGProcessor
from aws_config import AWSConfigDialog


class WorkerThread(QThread):
    """Background worker thread for handling long operations."""
    # Define the signals
    progress = pyqtSignal(str)  # Signal for progress updates
    finished = pyqtSignal(dict)  # Signal for completion
    error = pyqtSignal(str)  # Signal for errors

    def __init__(self, task_type: str, **kwargs):
        """Initialize worker thread."""
        super().__init__()
        self.task_type = task_type
        self.kwargs = kwargs

    def run(self):
        """Execute the specified task."""
        try:
            if self.task_type == "fetch_structure":
                api = OpenNeuroAPI()
                structure = api.get_file_structure(self.kwargs['accession_id'])
                self.finished.emit({"structure": structure})

            elif self.task_type == "download":
                api = OpenNeuroAPI()
                dataset_path = api.download_files(
                    self.kwargs['accession_id'],
                    self.kwargs['file_list'],
                    lambda x: self.progress.emit(x)
                )
                self.finished.emit({"path": str(dataset_path)})

            elif self.task_type == "process":
                processor = IEEGProcessor()
                results = processor.process_dataset(
                    self.kwargs['file_paths'],
                    self.kwargs['config'],
                    lambda x: self.progress.emit(x)
                )
                self.finished.emit({"results": results})

            elif self.task_type == "upload":
                aws_config = self.kwargs.get('aws_config', {})
                if not all([aws_config.get('access_key'),
                            aws_config.get('secret_key'),
                            aws_config.get('region'),
                            aws_config.get('bucket')]):
                    raise ValueError("Incomplete AWS configuration")

                s3 = S3Handler(aws_config)

                # Verify bucket permissions first
                success, message = s3.verify_bucket_permissions()
                if not success:
                    raise RuntimeError(f"S3 bucket access error: {message}")

                uploaded = s3.upload_processed_data(
                    self.kwargs['processed_results'],
                    self.kwargs['source_files'],
                    self.kwargs['prefix'],
                    lambda x: self.progress.emit(x)
                )
                self.finished.emit({"uploaded": uploaded})

        except Exception as e:
            logging.error(f"Worker thread error: {str(e)}")
            self.error.emit(str(e))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("OpenNeuro iEEG Processor")
        self.setMinimumSize(800, 600)
        self.setup_ui()
        self.current_thread = None
        self.dataset_path = None
        self.selected_files = []
        
        # Check AWS configuration
        self.check_aws_config()

    def setup_ui(self):
        """Set up the user interface."""
        # Setup menubar
        self.setup_menu_bar()

        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # Dataset input
        dataset_layout = QHBoxLayout()
        dataset_layout.addWidget(QLabel("Dataset ID:"))
        self.dataset_input = QLineEdit()
        self.dataset_input.setPlaceholderText("e.g., ds005691")
        dataset_layout.addWidget(self.dataset_input)
        self.fetch_button = QPushButton("Fetch Dataset")
        self.fetch_button.clicked.connect(self.fetch_dataset)
        dataset_layout.addWidget(self.fetch_button)
        layout.addLayout(dataset_layout)

        # File tree
        self.file_tree = QTreeView()
        self.file_model = QStandardItemModel()
        self.file_model.setHorizontalHeaderLabels(['Files'])
        self.file_tree.setModel(self.file_model)
        self.file_tree.setSelectionMode(QTreeView.SelectionMode.MultiSelection)
        layout.addWidget(QLabel("Available Files:"))
        layout.addWidget(self.file_tree)

        # Processing options
        options_layout = QVBoxLayout()
        options_layout.addWidget(QLabel("Processing Options:"))
        self.setup_processing_options(options_layout)
        layout.addLayout(options_layout)

        # Progress bar
        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)
        self.status_label = QLabel()
        layout.addWidget(self.status_label)

        # Process button
        self.process_button = QPushButton("Process Selected Files")
        self.process_button.clicked.connect(self.process_files)
        self.process_button.setEnabled(False)
        layout.addWidget(self.process_button)

    def setup_menu_bar(self):
        """Set up the application menu bar."""
        menubar = self.menuBar()
        
        # Settings Menu
        settings_menu = menubar.addMenu('Settings')
        aws_config_action = settings_menu.addAction('AWS Configuration')
        aws_config_action.triggered.connect(self.show_aws_config)
        
        # Help Menu
        help_menu = menubar.addMenu('Help')
        about_action = help_menu.addAction('About')
        about_action.triggered.connect(self.show_about)

    def setup_processing_options(self, layout: QVBoxLayout):
        """Set up processing options UI."""
        self.processing_config = {
            'power_spectra': True,
            'connectivity': True,
            'band_powers': True
        }
        
        for option in self.processing_config:
            btn = QPushButton(option.replace('_', ' ').title())
            btn.setCheckable(True)
            btn.setChecked(True)
            btn.toggled.connect(
                lambda checked, opt=option: self.toggle_processing_option(opt, checked)
            )
            layout.addWidget(btn)

    def check_aws_config(self):
        """Check if AWS is configured."""
        try:
            credentials = AWSConfigDialog.get_aws_credentials()
            if not all([credentials.get('access_key'), 
                       credentials.get('secret_key'),
                       credentials.get('region'),
                       credentials.get('bucket')]):
                self.prompt_aws_config()
        except Exception as e:
            self.prompt_aws_config()

    def prompt_aws_config(self):
        """Prompt user for AWS configuration."""
        response = QMessageBox.question(
            self,
            "AWS Configuration Required",
            "AWS configuration is required for storing results. Would you like to configure it now?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if response == QMessageBox.StandardButton.Yes:
            self.show_aws_config()
        else:
            QMessageBox.warning(
                self,
                "Warning",
                "AWS configuration is required for uploading results."
            )

    def show_aws_config(self):
        """Show AWS configuration dialog."""
        if AWSConfigDialog.get_aws_config(self):
            QMessageBox.information(
                self,
                "Success",
                "AWS configuration saved successfully!"
            )

    def show_about(self):
        """Show about dialog."""
        QMessageBox.about(
            self,
            "About OpenNeuro iEEG Processor",
            "A desktop application for processing iEEG data from OpenNeuro.\n\n"
            "Features:\n"
            "- Download datasets from OpenNeuro\n"
            "- Process iEEG data with various analyses\n"
            "- Secure storage of results in AWS S3\n\n"
            "Version 1.0.0"
        )

    def toggle_processing_option(self, option: str, state: bool):
        """Toggle processing option state."""
        self.processing_config[option] = state

    def fetch_dataset(self):
        """Fetch dataset structure and display in tree."""
        accession_id = self.dataset_input.text().strip()
        if not accession_id:
            QMessageBox.warning(self, "Error", "Please enter a dataset ID")
            return

        self.status_label.setText("Fetching dataset structure...")
        self.progress_bar.setRange(0, 0)
        self.fetch_button.setEnabled(False)

        self.current_thread = WorkerThread(
            "fetch_structure",
            accession_id=accession_id
        )
        self.current_thread.progress.connect(self.update_progress)
        self.current_thread.finished.connect(self.handle_structure_fetched)
        self.current_thread.error.connect(self.show_error)
        self.current_thread.start()

    def handle_structure_fetched(self, result: Dict):
        """Handle completion of dataset structure fetching."""
        structure = result.get('structure', {})
        self.populate_file_tree(structure)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100)
        self.fetch_button.setEnabled(True)
        self.process_button.setEnabled(True)
        self.status_label.setText("Select files to process")

    def populate_file_tree(self, structure: Dict):
        """Populate the file tree with dataset structure."""
        self.file_model.clear()
        self.file_model.setHorizontalHeaderLabels(['Files'])
        root = self.file_model.invisibleRootItem()

        for subject, files in structure.items():
            subject_item = QStandardItem(subject)
            subject_item.setCheckable(True)
            
            for file_path in sorted(files):
                file_item = QStandardItem(Path(file_path).name)
                file_item.setCheckable(True)
                file_item.setData(file_path, Qt.ItemDataRole.UserRole)
                subject_item.appendRow(file_item)
            
            root.appendRow(subject_item)
        
        self.file_tree.expandAll()

    def get_selected_files(self) -> List[str]:
        """Get list of selected files from the tree view."""
        selected_files = []
        
        def traverse_model(parent: QStandardItem):
            for row in range(parent.rowCount()):
                child = parent.child(row)
                if child.checkState() == Qt.CheckState.Checked:
                    file_path = child.data(Qt.ItemDataRole.UserRole)
                    if file_path:
                        selected_files.append(file_path)
                if child.hasChildren():
                    traverse_model(child)
        
        root = self.file_model.invisibleRootItem()
        traverse_model(root)
        return selected_files

    def process_files(self):
        """Process selected files."""
        selected_files = self.get_selected_files()
        if not selected_files:
            QMessageBox.warning(self, "Error", "Please select files to process")
            return

        try:
            credentials = AWSConfigDialog.get_aws_credentials()
            if not all([credentials.get('access_key'), 
                       credentials.get('secret_key'),
                       credentials.get('region'),
                       credentials.get('bucket')]):
                raise ValueError("AWS configuration is incomplete")

            self.selected_files = selected_files
            self.status_label.setText("Downloading selected files...")
            self.progress_bar.setRange(0, 0)
            self.process_button.setEnabled(False)

            self.current_thread = WorkerThread(
                "download",
                accession_id=self.dataset_input.text().strip(),
                file_list=selected_files
            )
            self.current_thread.progress.connect(self.update_progress)
            self.current_thread.finished.connect(self.handle_download_complete)
            self.current_thread.error.connect(self.show_error)
            self.current_thread.start()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                f"AWS configuration error: {str(e)}\n\n"
                "Please check your AWS settings."
            )

    def handle_download_complete(self, result: Dict):
        """Handle completion of file download."""
        self.dataset_path = Path(result['path'])
        self.status_label.setText("Processing files...")
        
        self.current_thread = WorkerThread(
            "process",
            file_paths=self.selected_files,
            config=self.processing_config
        )
        self.current_thread.progress.connect(self.update_progress)
        self.current_thread.finished.connect(self.handle_processing_complete)
        self.current_thread.error.connect(self.show_error)
        self.current_thread.start()

    def handle_processing_complete(self, result: Dict):
        """Handle completion of processing."""
        try:
            credentials = AWSConfigDialog.get_aws_credentials()
            if not all([credentials.get('access_key'), 
                       credentials.get('secret_key'),
                       credentials.get('region'),
                       credentials.get('bucket')]):
                raise ValueError("AWS configuration is incomplete")

            self.status_label.setText("Uploading results to S3...")
            
            self.current_thread = WorkerThread(
                "upload",
                processed_results=result.get('results', {}),
                source_files=self.selected_files,
                prefix=f"processed/{self.dataset_input.text()}",
                aws_config=credentials
            )
            self.current_thread.progress.connect(self.update_progress)
            self.current_thread.finished.connect(self.show_completion)
            self.current_thread.error.connect(self.show_error)
            self.current_thread.start()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                f"AWS configuration error: {str(e)}\n\n"
                "Please check your AWS settings."
            )

    def update_progress(self, message: str):
        """Update progress bar and status label."""
        self.status_label.setText(message)
        if "%" in message:
            try:
                percentage = float(message.split("%")[0].split(":")[-1].strip())
                self.progress_bar.setValue(int(percentage))
            except ValueError:
                pass

    def show_error(self, error_message: str):
        """Show error message dialog."""
        QMessageBox.critical(self, "Error", error_message)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.fetch_button.setEnabled(True)
        self.process_button.setEnabled(True)
        self.status_label.setText("Error occurred. Please try again.")


    def show_completion(self, result: Dict):
        """Show completion message."""
        QMessageBox.information(
            self,
            "Success",
            f"Processing and upload completed successfully!\n"
            f"Files uploaded: {len(result.get('uploaded', []))}"
        )
        self.progress_bar.setValue(0)
        self.status_label.clear()
        self.process_button.setEnabled(True)

    def closeEvent(self, event):
        """Handle application closure."""
        try:
            if self.current_thread and self.current_thread.isRunning():
                response = QMessageBox.question(
                    self,
                    "Confirm Exit",
                    "A process is still running. Do you want to stop it and exit?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )

                if response == QMessageBox.StandardButton.Yes:
                    self.current_thread.terminate()
                    self.current_thread.wait()
                else:
                    event.ignore()
                    return

            # Clean up temporary files if needed
            if self.dataset_path and self.dataset_path.exists():
                try:
                    import shutil
                    shutil.rmtree(self.dataset_path)
                except Exception as e:
                    logging.warning(f"Failed to clean up temporary files: {str(e)}")

            event.accept()

        except Exception as e:
            logging.error(f"Error during application shutdown: {str(e)}")
            event.accept()


def main():
    """Main application entry point."""
    try:
        app = QApplication(sys.argv)

        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('openneuro_app.log')
            ]
        )

        # Create and show the main window
        window = MainWindow()
        window.show()

        # Start the event loop
        sys.exit(app.exec())

    except Exception as e:
        logging.critical(f"Application failed to start: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()