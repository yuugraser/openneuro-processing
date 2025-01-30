"""
AWS S3 upload handler with secure credential management.
Implements efficient multipart uploads and error handling.
"""
import boto3
import logging
import json
import tempfile
from datetime import datetime
from typing import List, Optional, Dict, Tuple
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError, CredentialRetrievalError

class S3Handler:
    def __init__(self, aws_config: Dict):
        """
        Initialize S3 client using provided configuration.
        
        Args:
            aws_config: Dictionary containing AWS configuration
                     {'access_key': str, 'secret_key': str, 
                      'region': str, 'bucket': str}
        
        Raises:
            ValueError: If AWS configuration is incomplete
            RuntimeError: If AWS client initialization fails
        """
        required_keys = ['access_key', 'secret_key', 'region', 'bucket']
        missing_keys = [key for key in required_keys if not aws_config.get(key)]
        
        if missing_keys:
            raise ValueError(
                f"Missing required AWS configuration: {', '.join(missing_keys)}"
            )
        
        try:
            self.session = boto3.Session(
                aws_access_key_id=aws_config['access_key'],
                aws_secret_access_key=aws_config['secret_key'],
                region_name=aws_config['region']
            )
            self.s3 = self.session.client('s3')
            self.bucket = aws_config['bucket']
            
            # Verify bucket access
            self.s3.head_bucket(Bucket=self.bucket)
            
        except (NoCredentialsError, CredentialRetrievalError) as e:
            raise RuntimeError(f"AWS credentials error: {str(e)}")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404':
                raise RuntimeError(f"Bucket {self.bucket} does not exist")
            elif error_code == '403':
                raise RuntimeError(f"Access denied to bucket {self.bucket}")
            else:
                raise RuntimeError(f"AWS S3 error: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize AWS S3 client: {str(e)}")

    def verify_bucket_permissions(self) -> Tuple[bool, str]:
        """
        Verify read/write permissions for the configured bucket.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Try to list objects (read permission)
            self.s3.list_objects_v2(Bucket=self.bucket, MaxKeys=1)
            
            # Try to create a test object (write permission)
            test_key = f"test/permission_check_{datetime.now().timestamp()}"
            self.s3.put_object(
                Bucket=self.bucket,
                Key=test_key,
                Body=b"permission_check"
            )
            
            # Clean up test object
            self.s3.delete_object(Bucket=self.bucket, Key=test_key)
            
            return True, "Bucket permissions verified"
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            return False, f"Bucket permission check failed: {error_code}"
        except Exception as e:
            return False, f"Bucket permission check failed: {str(e)}"

    def upload_file(self, file_path: Path, s3_key: str,
                   progress_callback: Optional[callable] = None) -> bool:
        """
        Upload a single file to S3 with progress tracking.
        
        Args:
            file_path: Path to local file
            s3_key: Destination key in S3
            progress_callback: Optional progress callback
            
        Returns:
            bool indicating success
        """
        try:
            file_size = file_path.stat().st_size
            
            # Use multipart upload for files larger than 8MB
            if file_size > 8 * 1024 * 1024:
                return self._multipart_upload(file_path, s3_key, progress_callback)
            
            # Regular upload for smaller files
            with open(file_path, 'rb') as f:
                if progress_callback:
                    self.s3.upload_fileobj(
                        f,
                        self.bucket,
                        s3_key,
                        Callback=lambda bytes_transferred: 
                            progress_callback(bytes_transferred / file_size * 100)
                    )
                else:
                    self.s3.upload_fileobj(f, self.bucket, s3_key)
                    
            return True
            
        except Exception as e:
            logging.error(f"Failed to upload {file_path}: {str(e)}")
            return False

    def _multipart_upload(self, file_path: Path, s3_key: str,
                         progress_callback: Optional[callable] = None) -> bool:
        """
        Handle multipart upload for large files.
        
        Args:
            file_path: Path to local file
            s3_key: Destination key in S3
            progress_callback: Optional progress callback
            
        Returns:
            bool indicating success
        """
        mpu = None
        try:
            file_size = file_path.stat().st_size
            
            # Initialize multipart upload
            mpu = self.s3.create_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key
            )
            
            # Upload parts
            chunk_size = 8 * 1024 * 1024  # 8MB chunks
            parts = []
            uploaded_bytes = 0
            
            with open(file_path, 'rb') as f:
                part_num = 1
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        break
                    
                    # Upload part
                    part = self.s3.upload_part(
                        Bucket=self.bucket,
                        Key=s3_key,
                        PartNumber=part_num,
                        UploadId=mpu['UploadId'],
                        Body=data
                    )
                    
                    parts.append({
                        'PartNumber': part_num,
                        'ETag': part['ETag']
                    })
                    
                    uploaded_bytes += len(data)
                    if progress_callback:
                        progress_callback(uploaded_bytes / file_size * 100)
                    
                    part_num += 1
            
            # Complete multipart upload
            self.s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )
            
            return True
            
        except Exception as e:
            logging.error(f"Multipart upload failed for {s3_key}: {str(e)}")
            if mpu:
                try:
                    self.s3.abort_multipart_upload(
                        Bucket=self.bucket,
                        Key=s3_key,
                        UploadId=mpu['UploadId']
                    )
                except Exception:
                    pass
            return False

    def upload_processed_data(self, 
                            processed_results: Dict,
                            source_files: List[str],
                            prefix: str,
                            progress_callback: Optional[callable] = None) -> List[str]:
        """
        Upload processing results and source files.
        
        Args:
            processed_results: Dictionary of processing results
            source_files: List of source file paths
            prefix: S3 key prefix
            progress_callback: Optional progress callback
            
        Returns:
            List of uploaded S3 keys
            
        Raises:
            RuntimeError: If upload fails
        """
        uploaded_keys = []
        total_files = len(source_files) + 1  # +1 for results file
        files_processed = 0

        try:
            # Verify bucket permissions first
            success, message = self.verify_bucket_permissions()
            if not success:
                raise RuntimeError(f"Bucket permission check failed: {message}")

            # Create and upload results file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            with tempfile.NamedTemporaryFile(mode='w', 
                                           suffix='.json', 
                                           delete=False) as temp_file:
                json.dump(processed_results, temp_file, indent=2)
                temp_path = Path(temp_file.name)

            try:
                if progress_callback:
                    progress_callback("Uploading processing results")
                
                results_key = f"{prefix}/processing_results_{timestamp}.json"
                if self.upload_file(temp_path, results_key):
                    uploaded_keys.append(results_key)
                
                files_processed += 1
                if progress_callback:
                    progress_callback(f"Progress: {(files_processed/total_files)*100:.1f}%")
                
            finally:
                # Clean up temp file
                temp_path.unlink(missing_ok=True)

            # Upload source files
            for source_path in source_files:
                path = Path(source_path)
                if path.exists():
                    if progress_callback:
                        progress_callback(f"Uploading {path.name}")
                    
                    s3_key = f"{prefix}/source_files/{path.name}"
                    if self.upload_file(path, s3_key):
                        uploaded_keys.append(s3_key)
                    
                    files_processed += 1
                    if progress_callback:
                        progress_callback(f"Progress: {(files_processed/total_files)*100:.1f}%")
                else:
                    logging.warning(f"Source file not found: {source_path}")

            if not uploaded_keys:
                raise RuntimeError("No files were successfully uploaded")

            return uploaded_keys

        except Exception as e:
            logging.error(f"Upload error: {str(e)}")
            raise RuntimeError(f"Failed to upload to S3: {str(e)}")