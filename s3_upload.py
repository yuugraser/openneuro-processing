"""
AWS S3 upload handler with secure credential management.
Implements efficient multipart uploads and error handling.
"""
import boto3
import logging
from typing import List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
from config import AWS_REGION, S3_BUCKET


class S3Handler:
    def __init__(self):
        """Initialize S3 client using AWS CLI credentials."""
        self.s3 = boto3.client('s3', region_name=AWS_REGION)
        self.bucket = S3_BUCKET

    def upload_file(self, file_path: Path, s3_key: str,
                    progress_callback: Optional[callable] = None) -> bool:
        """
        Upload a single file to S3 with multipart upload support.

        Args:
            file_path: Local path to file
            s3_key: Destination key in S3
            progress_callback: Optional callback for progress updates

        Returns:
            bool indicating success
        """
        try:
            if file_path.stat().st_size > 8 * 1024 * 1024:  # 8MB threshold
                self._multipart_upload(file_path, s3_key, progress_callback)
            else:
                self.s3.upload_file(
                    str(file_path),
                    self.bucket,
                    s3_key,
                    Callback=progress_callback
                )
            return True
        except ClientError as e:
            logging.error(f"Upload failed for {file_path}: {str(e)}")
            return False

    def _multipart_upload(self, file_path: Path, s3_key: str,
                          progress_callback: Optional[callable]) -> None:
        """
        Handle multipart upload for large files.

        Args:
            file_path: Local path to file
            s3_key: Destination key in S3
            progress_callback: Optional callback for progress updates
        """
        mpu = self.s3.create_multipart_upload(Bucket=self.bucket, Key=s3_key)

        try:
            parts = []
            uploaded_bytes = 0
            total_bytes = file_path.stat().st_size

            with open(file_path, 'rb') as f:
                i = 1
                while True:
                    data = f.read(8 * 1024 * 1024)  # 8MB chunks
                    if not data:
                        break

                    part = self.s3.upload_part(
                        Bucket=self.bucket,
                        Key=s3_key,
                        PartNumber=i,
                        UploadId=mpu['UploadId'],
                        Body=data
                    )

                    parts.append({
                        'PartNumber': i,
                        'ETag': part['ETag']
                    })

                    uploaded_bytes += len(data)
                    if progress_callback:
                        progress_callback(uploaded_bytes / total_bytes * 100)

                    i += 1

            self.s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )

        except Exception as e:
            self.s3.abort_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key,
                UploadId=mpu['UploadId']
            )
            raise e

    def upload_directory(self, local_path: Path, prefix: str,
                         progress_callback: Optional[callable] = None) -> List[str]:
        """
        Upload an entire directory to S3.

        Args:
            local_path: Local directory path
            prefix: S3 key prefix
            progress_callback: Optional callback for progress updates

        Returns:
            List of uploaded S3 keys
        """
        uploaded_keys = []
        files = list(local_path.rglob('*'))

        def upload_single_file(file_path: Path) -> Optional[str]:
            if file_path.is_file():
                rel_path = file_path.relative_to(local_path)
                s3_key = f"{prefix}/{rel_path}"
                if self.upload_file(file_path, s3_key, progress_callback):
                    return s3_key
            return None

        with ThreadPoolExecutor() as executor:
            results = executor.map(upload_single_file, files)
            uploaded_keys = [key for key in results if key]

        return uploaded_keys