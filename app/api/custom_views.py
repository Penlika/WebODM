# Custom views for WebODM API
from django.utils import timezone
from rest_framework.authentication import BasicAuthentication
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from app.models import Project
from app.api.serializers import PayloadContainerSerializer
from app.api.remote_tasks import task_download_and_process
import logging
from zoneinfo import ZoneInfo
from django.utils import timezone

logger = logging.getLogger('webodm.app')

@method_decorator(csrf_exempt, name='dispatch')
class ProjectCreateFromRemote(APIView):
    """
    API endpoint to create a project from remote data.
    """
    authentication_classes = [BasicAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, format=None):
        """
        Provides information about this endpoint when accessed via GET request.
        """
        return Response({
            'message': 'This endpoint creates a project from remote data.',
            'method': 'POST',
            'required_query_params': {
                'host': 'The MinIO host URL (e.g., http://10.6.11.251:9000/)'
            },
            'required_body': 'JSON array of data containers with task and file information',
            'authentication': 'Basic Authentication required',
            'example_usage': 'POST /api/projects/create_from_remote/?host=http://example.com:9000/',
            'response': 'Returns project_id and project_name on success (HTTP 202)',
            'status': 'Endpoint registered and working!'
        }, status=status.HTTP_200_OK)

    def post(self, request, format=None):
        """
        Handles the creation of a project from remote JSON data.
        """
        # 1. Validate the 'host' query parameter
        host = request.query_params.get('host')
        if not host:
            raise ValidationError('A "host" query parameter is required.')

        # 2. Handle array-wrapped payload: [{data: [...]}] or {data: [...]}
        payload_data = request.data
        if isinstance(payload_data, list) and len(payload_data) > 0:
            # Unwrap array: [{...}] -> {...}
            payload_data = payload_data[0]
        
        # 3. Validate the JSON body (supports both formats)
        serializer = PayloadContainerSerializer(data=payload_data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        data = serializer.validated_data

        # 3. Detect which format and extract project name and files
        if 'data' in data:
            # Format 1 (corrupted): {data: [{date, task, files: [{file_name, minio_path}]}]}
            first_item = data['data'][0]
            project_name = first_item['task']
            
            # Flatten all files from all data items
            all_object_paths = []
            for data_item in data['data']:
                for file_obj in data_item['files']:
                    all_object_paths.append(file_obj['minio_path'])
        else:
            # Format 2 (original): {TaskId, TaskName, payload: [{filename, object_path}]}
            try:
                if data['payload'] and 'folder' in data['payload'][0]:
                    project_name = data['payload'][0]['folder']
                else:
                    project_name = data['TaskName']
            except (IndexError, KeyError):
                project_name = data.get('TaskName', 'Unnamed_Project')
            
            # Extract all 'object_path' values into a single list
            all_object_paths = [file_item['object_path'] for file_item in data['payload']]

        # 4. Check for an existing project and rename it if found
        try:
            existing_project = Project.objects.get(name=project_name, owner=request.user)

            now_utc = timezone.now()
            vietnam_tz = ZoneInfo("Asia/Ho_Chi_Minh")
            now_vietnam = now_utc.astimezone(vietnam_tz)
            
            # A safer, file-system friendly format string
            timestamp = now_vietnam.strftime('%Y-%m-%d_%H-%M-%S')
            
            old_name = existing_project.name
            existing_project.name = f"{existing_project.name}_old_{timestamp}"
            existing_project.save()
            logger.info(f"Renamed existing project '{old_name}' to '{existing_project.name}'")
        except Project.DoesNotExist:
            pass

        # 5. Create the new project
        new_project = Project.objects.create(
            name=project_name,
            owner=request.user
        )
        logger.info(f"Created new project: {new_project.name} (ID: {new_project.id})")

        # 6. Validate we have files
        if not all_object_paths:
             # Clean up the newly created project since it will be empty
            new_project.delete()
            raise ValidationError("No file paths found in the payload.")

        logger.info(f"Extracted {len(all_object_paths)} file paths from payload")
        logger.info(f"Host: {host}")
        logger.info(f"Files to process: {all_object_paths[:5]}..." if len(all_object_paths) > 5 else f"Files: {all_object_paths}")

        # 7. Start the background task to download and process files
        task_result = task_download_and_process.delay(
            project_id=new_project.id,
            host=host,
            files=all_object_paths,
            options=[]
        )
        
        logger.info(f"Started background task {task_result.id} for project {new_project.id}")

        # 8. Respond with the new project's information
        return Response({
            'message': 'Project created and processing started.',
            'project_id': new_project.id,
            'project_name': new_project.name,
            'host': host,
            'file_count': len(all_object_paths),
            'task_id': str(task_result.id),
            'status': 'Files are being downloaded and processed in the background'
        }, status=status.HTTP_201_CREATED)
