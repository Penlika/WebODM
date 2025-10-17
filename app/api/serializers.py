# webodm/api/serializers.py
from rest_framework import serializers

# Format 1 (corrupted): {data: [{date, task, files: [{file_name, minio_path}]}]}
# Format 2 (original): {TaskId, TaskName, payload: [{filename, object_path}]}

class FileItemFlexSerializer(serializers.Serializer):
    # Format 2 fields (original)
    filename = serializers.CharField(required=False)
    object_path = serializers.CharField(required=False)
    field_name = serializers.CharField(required=False)
    content_type = serializers.CharField(required=False)
    folder = serializers.CharField(required=False)
    province = serializers.CharField(required=False)
    area = serializers.CharField(required=False)
    datetime = serializers.CharField(required=False)
    
    # Format 1 fields (corrupted)
    file_name = serializers.CharField(required=False)
    minio_path = serializers.CharField(required=False)

class DataItemSerializer(serializers.Serializer):
    date = serializers.CharField()
    task = serializers.CharField()
    files = FileItemFlexSerializer(many=True)

class PayloadContainerSerializer(serializers.Serializer):
    # Format 2 fields (original)
    TaskId = serializers.CharField(required=False)
    TaskName = serializers.CharField(required=False)
    payload = FileItemFlexSerializer(many=True, required=False)
    
    # Format 1 fields (corrupted)
    data = DataItemSerializer(many=True, required=False)
    prefix = serializers.CharField(required=False)
    
    def validate(self, attrs):
        has_format1 = 'data' in attrs
        has_format2 = 'payload' in attrs
        
        if not has_format1 and not has_format2:
            raise serializers.ValidationError("Either 'data' or 'payload' must be provided")
        
        return attrs