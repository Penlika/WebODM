# Celery tasks for remote file downloading and processing
import os
import tempfile
import shutil
import logging
from urllib.parse import urlparse
from celery.utils.log import get_task_logger
from django.conf import settings

from worker.celery import app

logger = get_task_logger(__name__)

@app.task(bind=True, name="app.api.remote_tasks.download_and_process")
def task_download_and_process(self, project_id, host, files, options):
    """
    Download files from MinIO and create a processing task.
    
    Args:
        project_id: The ID of the project to add the task to
        host: MinIO host URL (e.g., http://10.6.11.251:9000)
        files: List of full paths in format "bucket/path/to/file.jpg"
        options: Processing options for the task
    """
    # Import Django models inside the task to avoid initialization issues
    from app.models import Task
    from worker import tasks as worker_tasks
    
    try:
        from minio import Minio
        from minio.error import S3Error
    except ImportError:
        logger.error("Minio package not installed. Install it with: pip install minio")
        self.update_state(state='FAILURE', meta={
            'exc_type': 'ImportError',
            'exc_message': 'Minio package not installed'
        })
        raise ImportError("Minio package required for remote file downloads")

    # Create temporary directory for downloads
    temp_dir = tempfile.mkdtemp(prefix='remote_dl_', dir=os.path.join(settings.MEDIA_ROOT, 'tmp'))
    logger.info(f"Created temp directory: {temp_dir}")
    logger.info(f"Temp dir exists: {os.path.exists(temp_dir)}, writable: {os.access(temp_dir, os.W_OK)}")
    local_image_paths = []

    try:
        # Parse MinIO host URL
        parsed_host = urlparse(host)
        
        # Get MinIO credentials from settings (with defaults)
        access_key = getattr(settings, 'MINIO_ACCESS_KEY', 'minioadmin')
        secret_key = getattr(settings, 'MINIO_SECRET_KEY', 'minioadmin')
        
        # Initialize MinIO client
        minio_client = Minio(
            endpoint=parsed_host.netloc,
            access_key=access_key,
            secret_key=secret_key,
            secure=(parsed_host.scheme == 'https')
        )

        logger.info(f"Starting download of {len(files)} files from MinIO host {host}")
        self.update_state(state='PROGRESS', meta={'status': f'Downloading {len(files)} images...'})

        # Download each file
        for idx, full_path in enumerate(files):
            try:
                # Split bucket and object path (e.g., "uav-landing-zone/dong-nai/..." -> bucket="uav-landing-zone", object="dong-nai/...")
                parts = full_path.split('/', 1)
                if len(parts) != 2:
                    logger.error(f"Invalid path format: {full_path}. Expected 'bucket/path/to/file'")
                    continue
                
                bucket, object_path = parts
                local_filename = os.path.basename(object_path)
                local_path = os.path.join(temp_dir, local_filename)

                # Download the file from MinIO
                minio_client.fget_object(bucket, object_path, local_path)
                
                # Verify file was downloaded
                if os.path.exists(local_path):
                    file_size = os.path.getsize(local_path)
                    logger.info(f"Downloaded [{idx+1}/{len(files)}]: {local_filename} ({file_size} bytes)")
                    local_image_paths.append(local_path)
                else:
                    logger.error(f"Download reported success but file not found: {local_path}")
                
                logger.info(f"Downloaded [{idx+1}/{len(files)}]: s3://{bucket}/{object_path}")
                
                # Update progress
                progress = int((idx + 1) / len(files) * 100)
                self.update_state(state='PROGRESS', meta={
                    'status': f'Downloaded {idx+1}/{len(files)} files',
                    'progress': progress
                })
                
            except S3Error as e:
                logger.error(f"Failed to download {object_path}: {e}")
                # Continue with other files even if one fails

        if not local_image_paths:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise ValueError("No images were successfully downloaded")

        logger.info(f"Successfully downloaded {len(local_image_paths)} images to {temp_dir}")
        
        # Verify downloaded files exist
        verified_paths = []
        for img_path in local_image_paths:
            if os.path.exists(img_path):
                size = os.path.getsize(img_path)
                verified_paths.append(img_path)
                logger.info(f"Verified: {os.path.basename(img_path)} ({size} bytes)")
            else:
                logger.error(f"MISSING: {img_path} was reported downloaded but doesn't exist!")
        
        if not verified_paths:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise ValueError(f"Downloaded {len(local_image_paths)} files but none exist on disk!")
        
        logger.info(f"Verified {len(verified_paths)}/{len(local_image_paths)} files exist on disk")
        local_image_paths = verified_paths

        # Create a processing task with the downloaded images
        task = Task.objects.create(
            project_id=project_id,
            name=f"Task from MinIO ({len(local_image_paths)} images)",
            processing_node=None,  # Will auto-assign
            auto_processing_node=True
        )

        # Create task directories
        task.create_task_directories()

        # Move images to task directory (task root, not subdirectory!)
        dest_dir = task.task_path()
        
        logger.info(f"Moving {len(local_image_paths)} images from {temp_dir} to {dest_dir}")
        
        moved_count = 0
        for img_path in local_image_paths:
            try:
                if not os.path.exists(img_path):
                    logger.error(f"Source file does not exist: {img_path}")
                    continue
                    
                dest_path = os.path.join(dest_dir, os.path.basename(img_path))
                shutil.move(img_path, dest_path)
                moved_count += 1
                
                if moved_count % 50 == 0:
                    logger.info(f"Moved {moved_count}/{len(local_image_paths)} images")
            except Exception as e:
                logger.error(f"Failed to move {img_path} to {dest_path}: {e}")
        
        logger.info(f"Successfully moved {moved_count}/{len(local_image_paths)} images to {dest_dir}")
        
        # Verify files are in destination
        actual_files = len([f for f in os.listdir(dest_dir) if os.path.isfile(os.path.join(dest_dir, f))])
        logger.info(f"Verification: {actual_files} files found in {dest_dir}")
        
        # CRITICAL: Manually update the task's images_count
        task.images_count = actual_files
        task.save()
        logger.info(f"Updated task.images_count to {task.images_count}")
        
        # Clean up temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)

        logger.info(f"Created task {task.id} with {actual_files} images at {dest_dir}")

        # Start processing
        worker_tasks.process_task.delay(task.id)
        
        logger.info(f"Started processing task {task.id}")

        return {
            'status': 'success',
            'task_id': task.id,
            'images_downloaded': len(local_image_paths)
        }

    except Exception as e:
        # Clean up on error
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        
        logger.error(f"Task failed: {e}")
        self.update_state(state='FAILURE', meta={
            'exc_type': type(e).__name__,
            'exc_message': str(e)
        })
        raise