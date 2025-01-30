"""
AWS S3 upload handler with secure credential management.
Implements efficient multipart uploads and error handling.
"""
import boto3
import logging
from typing import List, Optional, Dict
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
from config import AWS_REGION, S3_BUCKET

class S3Handler:
    def __init__(self):
        """Initialize S3 client using AWS CLI credentials."""
        self.s3 = boto3.client('s3', region_name=AWS_REGION)
        self.bucket = S3_BUCKET

    def upload_processed_data(self,
                            processed_results: Dict,
                            source_files: List[str],
                            prefix: str,
                            progress_callback: Optional[callable] = None) -> List[str]:
        """
        Upload only processed data and selected source files to S3.

        Args:
            processed_results: Dictionary containing processing results
            source_files: List of original selected file paths
            prefix: S3 key prefix
            progress_callback: Optional callback for progress updates

        Returns:
            List of uploaded S3 keys
        """
        try:
            uploaded_keys = []
            total_files = len(source_files) + 1  # +1 for results file
            files_processed = 0

            # Convert source files to Path objects for consistent handling
            source_paths = [Path(f) for f in source_files]

            # Upload the processing results as JSON
            import json
            import tempfile
            from datetime import datetime

            # Create results file
            results_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
            with open(results_file.name, 'w') as f:
                json.dump(processed_results, f)

            # Upload results
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            results_key = f"{prefix}/processing_results_{timestamp}.json"

            if progress_callback:
                progress_callback(f"Uploading processing results")

            self.s3.upload_file(results_file.name, self.bucket, results_key)
            uploaded_keys.append(results_key)
            files_processed += 1

            if progress_callback:
                progress_callback(f"Progress: {(files_processed/total_files)*100:.1f}%")

            # Upload only the selected source files
            for source_path in source_paths:
                if source_path.exists():
                    # Maintain the subject/session directory structure but only for selected files
                    rel_path = source_path.name  # Just use filename to keep it flat
                    s3_key = f"{prefix}/source_files/{rel_path}"

                    if progress_callback:
                        progress_callback(f"Uploading {source_path.name}")

                    self.s3.upload_file(str(source_path), self.bucket, s3_key)
                    uploaded_keys.append(s3_key)

                    files_processed += 1
                    if progress_callback:
                        progress_callback(f"Progress: {(files_processed/total_files)*100:.1f}%")

            # Cleanup
            Path(results_file.name).unlink()

            return uploaded_keys

        except Exception as e:
            logging.error(f"Upload error: {str(e)}")
            raise RuntimeError(f"Failed to upload to S3: {str(e)}")