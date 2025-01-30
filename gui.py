"""
PyQt6-based GUI for the OpenNeuro Desktop Application.
Implements user interface for data selection and processing.
"""
import sys
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                            QHBoxLayout, QLabel, QLineEdit, QPushButton,
                            QProgressBar, QFileDialog, QTreeView, QMessageBox)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QDir
from PyQt6.QtGui import QStandardItemModel, QStandardItem
from pathlib import Path
from typing import List, Dict
from fetch_data import OpenNeuroAPI
from s3_upload import S3Handler
from process_ieeg import IEEGProcessor
import os

class WorkerThread(QThread):
    """Worker thread for handling long-running operations."""
    progress = pyqtSignal(str)
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)

    def __init__(self, task_type: str, **kwargs):
        super().__init__()
        self.task_type = task_type
        self.kwargs = kwargs

    def run(self):
        try:
            if self.task_type == "fetch":
                api = OpenNeuroAPI()
                dataset_path = api.download_files(
                    self.kwargs['accession_id'],
                    [],  # Empty list to fetch metadata without downloading files
                    lambda x: self.progress.emit(f"Fetching: {x}")
                )
                self.finished.emit({"path": str(dataset_path)})

            elif self.task_type == "upload":
                s3 = S3Handler()
                uploaded = s3.upload_directory(
                    Path(self.kwargs['local_path']),
                    self.kwargs['prefix'],
                    lambda x: self.progress.emit(f"Uploading: {x:.1f}%")
                )
                self.finished.emit({"uploaded": uploaded})

            elif self.task_type == "process":
                processor = IEEGProcessor()
                results = processor.process_dataset(
                    self.kwargs['file_paths'],
                    self.kwargs['config'],
                    lambda x: self.progress.emit(f"Processing: {x}")
                )
                self.finished.emit({"results": results})

        except Exception as e:
            self.error.emit(str(e))

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("OpenNeuro iEEG Processor")
        self.setMinimumSize(800, 600)
        self.setup_ui()
        self.dataset_path = None

    def setup_ui(self):
        """Set up the main user interface."""
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

        # File selection
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

    def populate_file_tree(self, path: Path):
        """Populate the file tree with the dataset structure."""
        self.file_model.clear()
        self.file_model.setHorizontalHeaderLabels(['Files'])
        root = self.file_model.invisibleRootItem()

        def add_directory(parent: QStandardItem, dir_path: Path):
            for item in sorted(dir_path.iterdir()):
                child = QStandardItem(item.name)
                child.setCheckable(True)
                if item.is_file():
                    child.setData(str(item), Qt.ItemDataRole.UserRole)
                else:
                    add_directory(child, item)
                parent.appendRow(child)

        try:
            add_directory(root, path)
            self.file_tree.expandAll()
            self.process_button.setEnabled(True)
            self.status_label.setText("Dataset fetched successfully. Select files to process.")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to populate file tree: {str(e)}")

    def setup_processing_options(self, layout: QVBoxLayout):
        """Set up processing options UI."""
        self.processing_config = {
            'remove_artifacts': True,
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

    def toggle_processing_option(self, option: str, state: bool):
        """Toggle processing option state."""
        self.processing_config[option] = state

    def fetch_dataset(self):
        """Fetch dataset metadata and update UI."""
        accession_id = self.dataset_input.text().strip()
        if not accession_id:
            QMessageBox.warning(self, "Error", "Please enter a dataset ID")
            return

        self.status_label.setText("Fetching dataset...")
        self.progress_bar.setRange(0, 0)  # Show indeterminate progress
        self.fetch_button.setEnabled(False)

        self.worker = WorkerThread(
            "fetch",
            accession_id=accession_id
        )
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.handle_fetch_complete)
        self.worker.error.connect(self.show_error)
        self.worker.start()

    def handle_fetch_complete(self, result: Dict):
        """Handle completion of dataset fetching."""
        self.dataset_path = Path(result['path'])
        self.populate_file_tree(self.dataset_path)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100)
        self.fetch_button.setEnabled(True)

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

        self.worker = WorkerThread(
            "process",
            file_paths=selected_files,
            config=self.processing_config
        )
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.handle_processed_data)
        self.worker.error.connect(self.show_error)
        self.worker.start()

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
        self.status_label.setText("Error occurred. Please try again.")

    def handle_processed_data(self, results: Dict):
        """Handle processed data and initiate upload."""
        self.worker = WorkerThread(
            "upload",
            local_path=self.dataset_path,
            prefix=f"processed/{self.dataset_input.text()}"
        )
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.show_completion)
        self.worker.error.connect(self.show_error)
        self.worker.start()

    def show_completion(self, result: Dict):
        """Show completion message."""
        QMessageBox.information(
            self,
            "Success",
            "Processing and upload completed successfully!"
        )
        self.progress_bar.setValue(0)
        self.status_label.clear()

    def closeEvent(self, event):
        """Handle application closure."""
        if hasattr(self, 'worker') and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()
        event.accept()