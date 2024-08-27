from src.utils import s3Connection


# List files in an S3 bucket
def listFilesInS3(bucket, aws_access_key_id, aws_secret_access_key, region_name):
    s3Client = s3Connection.createS3Client(aws_access_key_id, aws_secret_access_key, region_name)
    if s3Client is None:
        print("Could not create S3 client.")
        return []

    try:
        response = s3Client.list_objects_v2(Bucket=bucket)
        files = [item['Key'] for item in response.get('Contents', [])]
        return files
    except Exception as e:
        print(f"Error listing files: {e}")
        return []
