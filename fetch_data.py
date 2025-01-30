"""
OpenNeuro API handler for fetching iEEG data.
Implements efficient data retrieval and caching mechanisms.
"""
import os
import json
import requests
import subprocess
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import datalad.api as dl
from concurrent.futures import ThreadPoolExecutor
from packaging import version
from config import OPENNEURO_API_URL, TEMP_DIR, MAX_CONCURRENT_DOWNLOADS
import logging
import shutil

def check_git_annex() -> Tuple[bool, str]:
    """Check if git-annex is installed and meets version requirements."""
    try:
        result = subprocess.run(['git-annex', 'version'],
                              capture_output=True,
                              text=True)

        if result.returncode != 0:
            return False, "git-annex is not installed"

        version_line = [line for line in result.stdout.split('\n')
                       if line.startswith('git-annex version:')][0]
        current_version = version_line.split(':')[1].strip()

        if version.parse(current_version) < version.parse("8.20200309"):
            return False, f"git-annex version {current_version} is too old. Version >= 8.20200309 required"

        return True, ""

    except FileNotFoundError:
        return False, "git-annex is not installed"
    except Exception as e:
        return False, f"Error checking git-annex: {str(e)}"

class OpenNeuroAPI:
    def __init__(self):
        self.base_url = OPENNEURO_API_URL
        self.session = requests.Session()

        # Check git-annex on initialization
        is_valid, error_msg = check_git_annex()
        if not is_valid:
            raise RuntimeError(
                f"git-annex configuration error: {error_msg}\n"
                "Please visit http://handbook.datalad.org/r.html?install "
                "for instructions on how to install DataLad and git-annex."
            )

    def _setup_dataset(self, accession_id: str,
                      progress_callback: Optional[callable] = None) -> dl.Dataset:
        """
        Set up a DataLad dataset for OpenNeuro data.

        Args:
            accession_id: Dataset accession number
            progress_callback: Optional callback for progress updates

        Returns:
            DataLad Dataset object
        """
        dataset_path = TEMP_DIR / accession_id

        # Clean up any existing failed clone
        if dataset_path.exists():
            shutil.rmtree(dataset_path)

        if progress_callback:
            progress_callback(f"Cloning dataset {accession_id}")

        # Clone the dataset
        dataset = dl.clone(
            source=f"https://github.com/OpenNeuroDatasets/{accession_id}.git",
            path=dataset_path,
            result_renderer='disabled'
        )

        if progress_callback:
            progress_callback("Configuring dataset")

        # Configure the dataset
        dataset.config.set(
            'datalad.get.subdataset-source-candidate-200',
            'https://github.com/OpenNeuroDatasets/{}',
            where='local'
        )

        # Configure remotes for S3 access
        remotes = dataset.repo.get_remotes()
        if 'openneuro-s3' not in remotes:
            dataset.repo.add_remote(
                'openneuro-s3',
                f'https://s3.amazonaws.com/openneuro.org/{accession_id}'
            )

        return dataset

    def download_files(self, accession_id: str, file_list: List[str],
                      progress_callback: Optional[callable] = None) -> Path:
        """
        Download selected files using DataLad.

        Args:
            accession_id: Dataset accession number
            file_list: List of files to download
            progress_callback: Optional callback for progress updates

        Returns:
            Path to downloaded files
        """
        try:
            # Set up the dataset
            dataset = self._setup_dataset(accession_id, progress_callback)
            dataset_path = Path(dataset.path)

            def download_file(file_path: str) -> None:
                """Download a single file using DataLad get."""
                try:
                    if progress_callback:
                        progress_callback(f"Downloading {file_path}")

                    # Use DataLad's get command
                    dataset.get(
                        file_path,
                        dataset=True,
                        result_renderer='disabled'
                    )

                except Exception as e:
                    logging.error(f"Error downloading {file_path}: {str(e)}")
                    raise

            # If no specific files are listed, get everything
            if not file_list:
                if progress_callback:
                    progress_callback("Downloading all dataset files")
                dataset.get('.', recursive=True, result_renderer='disabled')
            else:
                # Download specified files in parallel
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
                    futures = [executor.submit(download_file, f) for f in file_list]
                    for future in futures:
                        future.result()

            return dataset_path

        except Exception as e:
            logging.error(f"Error during dataset download: {str(e)}")
            raise RuntimeError(f"Error downloading dataset: {str(e)}")

    def get_dataset_metadata(self, accession_id: str) -> Dict:
        """Fetch dataset metadata from OpenNeuro."""
        url = f"{self.base_url}/datasets/{accession_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def list_files(self, accession_id: str, version: str) -> List[Dict]:
        """List all files in a dataset version."""
        url = f"{self.base_url}/datasets/{accession_id}/versions/{version}/files"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        """Clean up resources."""
        self.session.close()