import os
import json
import requests
import subprocess
from typing import List, Dict, Optional, Tuple, Set
from pathlib import Path
import datalad.api as dl
from concurrent.futures import ThreadPoolExecutor
from packaging import version
from config import TEMP_DIR, MAX_CONCURRENT_DOWNLOADS
import logging

class OpenNeuroAPI:
    def __init__(self):
        """Initialize OpenNeuro API handler."""
        self.base_url = "https://openneuro.org/crn/datasets"
        self.session = requests.Session()

    def _verify_file_exists(self, dataset_path: Path, file_path: str) -> Optional[Path]:
        """
        Verify if a file exists in the repository and return its correct path.
        
        Args:
            dataset_path: Path to the dataset
            file_path: Relative path to the file
            
        Returns:
            Path object if file exists, None otherwise
        """
        # Convert to Path object and normalize
        try:
            file_path = Path(file_path)
            
            # Try different path combinations
            possible_paths = [
                dataset_path / file_path,  # Direct path
                dataset_path / file_path.name,  # Just the filename
                dataset_path / file_path.parent.name / file_path.name  # Parent dir + filename
            ]
            
            # Also check for .git-annex files
            for path in possible_paths:
                if path.exists():
                    return path.relative_to(dataset_path)
                # Check for git-annex pointer file
                git_annex_path = path.parent / f"{path.stem}.git-annex"
                if git_annex_path.exists():
                    return path.relative_to(dataset_path)
                
            # If not found, search the directory tree
            for root, _, files in os.walk(dataset_path):
                if file_path.name in files:
                    found_path = Path(root) / file_path.name
                    return found_path.relative_to(dataset_path)
                    
            return None
            
        except Exception as e:
            logging.error(f"Error verifying file path {file_path}: {str(e)}")
            return None

    def _get_related_files(self, file_path: Path) -> Set[Path]:
        """
        Get related BIDS files for a given file.
        
        Args:
            file_path: Path to the main file
            
        Returns:
            Set of related file paths
        """
        related_files = set()
        
        # Get the base components
        parts = list(file_path.parts)
        if len(parts) >= 2:  # At least subject/file
            # Find the base filename without the type suffix
            base_name = file_path.name
            for suffix in ['_ieeg.tsv', '_ieeg.json', '_channels.tsv', '_events.tsv']:
                base_name = base_name.replace(suffix, '')
            
            # Add all possible related files
            parent_path = file_path.parent
            related_files.update([
                parent_path / f"{base_name}_ieeg.json",
                parent_path / f"{base_name}_ieeg.tsv",
                parent_path / f"{base_name}_channels.tsv",
                parent_path / f"{base_name}_events.tsv"
            ])
        
        return related_files

    def download_files(self, accession_id: str, file_list: List[str], 
                      progress_callback: Optional[callable] = None) -> Path:
        """
        Download only selected files and their necessary companions.
        """
        dataset_path = TEMP_DIR / accession_id
        
        try:
            if progress_callback:
                progress_callback(f"Setting up dataset {accession_id}")
            
            # Clone or update the dataset
            if not dataset_path.exists():
                dl.clone(
                    source=f"https://github.com/OpenNeuroDatasets/{accession_id}",
                    path=dataset_path,
                    reckless='availability',
                    description='false',
                    result_renderer='disabled'
                )

            # Initialize git-annex
            subprocess.run(['git', 'annex', 'init'], 
                         cwd=dataset_path, 
                         capture_output=True)

            # Verify and collect files to download
            valid_files = set()
            for file_path in file_list:
                file_path = Path(file_path)
                verified_path = self._verify_file_exists(dataset_path, file_path)
                if verified_path:
                    valid_files.add(verified_path)
                    # Add related files
                    for related_path in self._get_related_files(verified_path):
                        if self._verify_file_exists(dataset_path, related_path):
                            valid_files.add(related_path)

            if not valid_files:
                raise RuntimeError("No valid files found to download")

            if progress_callback:
                progress_callback(f"Found {len(valid_files)} files to download")

            # Download files
            dataset = dl.Dataset(dataset_path)
            for idx, file_path in enumerate(valid_files, 1):
                try:
                    if progress_callback:
                        progress_callback(f"Downloading file {idx}/{len(valid_files)}: {file_path}")
                    
                    dataset.get(path=str(file_path))
                    
                except Exception as e:
                    logging.error(f"Failed to download {file_path}: {str(e)}")
                    continue

            return dataset_path

        except Exception as e:
            logging.error(f"Error during dataset download: {str(e)}")
            raise RuntimeError(f"Error downloading dataset: {str(e)}")

    def get_file_structure(self, accession_id: str) -> Dict:
        """Get dataset file structure without downloading data."""
        try:
            temp_path = TEMP_DIR / accession_id
            if not temp_path.exists():
                dl.clone(
                    source=f"https://github.com/OpenNeuroDatasets/{accession_id}",
                    path=temp_path,
                    reckless='availability',
                    description='false',
                    result_renderer='disabled'
                )

            structure = {}
            
            # Walk through the directory structure
            for root, _, files in os.walk(temp_path):
                for file in files:
                    if any(ext in file for ext in ['_ieeg.tsv', '_ieeg.json', '_channels.tsv', '_events.tsv']):
                        rel_path = os.path.relpath(os.path.join(root, file), temp_path)
                        parts = Path(rel_path).parts
                        
                        # Find the subject directory (starts with 'sub-')
                        subject = next((p for p in parts if p.startswith('sub-')), None)
                        
                        if subject:
                            if subject not in structure:
                                structure[subject] = []
                            structure[subject].append(rel_path)

            return structure

        except Exception as e:
            logging.error(f"Error getting file structure: {str(e)}")
            raise RuntimeError(f"Failed to get file structure: {str(e)}")

    def close(self) -> None:
        """Clean up resources."""
        self.session.close()
