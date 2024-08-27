import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Function to establish an S3 connection
def createS3Client(awsAccessKeyId, awsSecretAccessKey, regionName):
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=awsAccessKeyId,
            aws_secret_access_key=awsSecretAccessKey,
            region_name=regionName
        )
        return s3
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
        return None