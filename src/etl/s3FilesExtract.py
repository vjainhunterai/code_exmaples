
import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from botocore.exceptions import BotoCoreError, ClientError


def getS3Directories(daysS3):
    """
    Generate a list of directory names for the current and, if applicable, the previous month
    based on the day of the month and a provided cutoff day.

    Args:
        daysS3 (int): The cutoff day of the current month. If today's date is less than or equal to this value,
                      the directory for the previous month will also be included.

    Returns:
        List[str]: A list containing the S3 directory paths for the current month and, if applicable, the previous month.
                   Each path follows the format 'customer_data/<MM_Data_Upload_MonthYear>'.
    """
    try:
        today = datetime.today()
        currentMonthDir = today.strftime("%m_Data_Upload_%B%Y")
        if today.day <= daysS3:
            previousMonth = today - timedelta(days=today.day + 1)
            previousMonthDir = previousMonth.strftime("%m_Data_Upload_%B%Y")
            return [f'customer_data/{currentMonthDir}', f'customer_data/{previousMonthDir}']
        else:
            return [f'customer_data/{currentMonthDir}']
    except Exception as e:
        print(f"Failed to generate S3 directories: {e}")
        return []

def listFilesInS3Directories(directories, s3Client, bucketName):
    """
    List all files in the specified S3 directories, filtering out directory keys and extracting the filenames.

    Args:
        directories (List[str]): A list of S3 directory paths to search for files.
        s3Client (boto3.client): A boto3 S3 client object used to interact with the S3 service.
        bucketName (str): The name of the S3 bucket where the directories reside.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing the 'Key' (filename) and 'LastModified' timestamp of the file.
    """
    files = []
    try:
        for directory in directories:
            response = s3Client.list_objects_v2(Bucket=bucketName, Prefix=directory)
            for obj in response.get('Contents', []):
                key = obj['Key']
                if not key.endswith('/'):  # Filter out directory keys
                    filename = os.path.basename(key)  # Extract filename from key
                    files.append({'Key': filename, 'LastModified': obj['LastModified']})
    except (BotoCoreError, ClientError) as e:
        print(f"Failed to list files in S3 directories: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while listing files: {e}")
    return files

def getIncrementalFiles(s3Files, auditFiles):
    """
    Identify new files in the S3 directories by comparing them with previously processed files listed in an audit table.

    Args:
        s3Files (List[Dict[str, Any]]): A list of dictionaries representing files from the S3 directories, with keys 'Key' and 'LastModified'.
        auditFiles (pd.DataFrame): A DataFrame containing file records from the audit table, with at least a 'file_name' column.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the incremental files that are new and not present in the audit table.
    """
    try:
        s3FilesDf = pd.DataFrame(s3Files)
        auditFiles.rename(columns={'file_name': 'Key'}, inplace=True)
        # Extract only the 'Key' column for comparison
        auditFiles = auditFiles['Key']
        auditFilesDf = pd.DataFrame(auditFiles, columns=['Key'])
        incrementalFilesDf = s3FilesDf[~s3FilesDf['Key'].isin(auditFilesDf['Key'])]
        return incrementalFilesDf.to_dict('records')
    except Exception as e:
        print(f"Failed to get incremental files: {e}")
        return []

def insertFilesToDb(incrementalFiles, s3FilesTable, dbConnection, insert):
    """
    Insert new (incremental) files into a MySQL table, marking them as ready for processing.

    Args:
        incrementalFiles (List[Dict[str, Any]]): A list of dictionaries representing incremental files to be inserted into the database.
        s3FilesTable (sqlalchemy.Table): The SQLAlchemy Table object representing the database table where files will be inserted.
        dbConnection (sqlalchemy.engine.Connection): A SQLAlchemy database connection object used to execute the insert operations.
        insert (sqlalchemy.sql.expression.Insert): A SQLAlchemy Insert expression used to build the insert statements.

    Returns:
        None
    """
    try:
        for file in incrementalFiles:
            stmt = insert(s3FilesTable).values(
                File_Name=file['Key'],
                Flag='Y',
                s3_load_time=datetime.now()
            )
            dbConnection.execute(stmt)
        dbConnection.commit()
    except SQLAlchemyError as e:
        print(f"Failed to insert files into the database: {e}")
        dbConnection.rollback()
    except Exception as e:
        print(f"An unexpected error occurred during database insertion: {e}")
    finally:
        dbConnection.close()

def fetchUnprocessedFiles(s3FilesTable, session, select):
    """
    Fetch unprocessed files from the MySQL table, where files are marked with the flag 'Y'.

    Args:
        s3FilesTable (sqlalchemy.Table): The SQLAlchemy Table object representing the database table where files are stored.
        session (sqlalchemy.orm.Session): A SQLAlchemy session object used to interact with the database.
        select (sqlalchemy.sql.expression.Select): A SQLAlchemy Select expression used to build the query for fetching records.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing an unprocessed file, with the file's details as key-value pairs.
    """
    try:
        # Create a query to fetch files with the flag 'Y'
        stmt = select(s3FilesTable).where(s3FilesTable.c.Flag == 'Y').limit(1)

        # Execute the query
        result = session.execute(stmt)

        # Convert each row to a dictionary
        unprocessedFilesDict = [row._asdict() for row in result]
        return unprocessedFilesDict
    except SQLAlchemyError as e:
        print(f"Failed to fetch unprocessed files from the database: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while fetching unprocessed files: {e}")
        return []
    finally:
        session.close()

def downloadAndProcessFile(s3Client, bucketName, unprocessedFile, listDir, localDirectory):
    """
    Download an unprocessed file from S3 to a local directory and process it.

    Args:
        s3Client (boto3.client): A boto3 S3 client object used to interact with the S3 service.
        bucketName (str): The name of the S3 bucket from which the file will be downloaded.
        unprocessedFile (Dict[str, Any]): A dictionary representing the unprocessed file, with details such as 'File_Name'.
        listDir (str): The S3 directory path containing the unprocessed file.
        localDirectory (str): The local directory path where the file will be saved.

    Returns:
        None

    Prints:
        str: A message indicating the success or failure of the file download operation.
    """
    # Combine S3 directory and file name to get the full S3 path using forward slashes
    s3FilePath = f"{listDir}/{unprocessedFile['File_Name']}"

    # Create the local file path using os.path.join
    localFilePath = f"{localDirectory}/{unprocessedFile['File_Name']}"

    # Debugging: Print the S3 path and local path
    print(f"Attempting to download from S3 path: {s3FilePath}")
    print(f"Local file path: {localFilePath}")

    # Download the file from S3 to the local directory
    try:
        s3Client.download_file(bucketName, s3FilePath, localFilePath)
        print(f"Downloaded {s3FilePath} to {localFilePath}")
    except s3Client.exceptions.NoSuchKey:
        print(f"File not found in S3: {s3FilePath}")
    except (BotoCoreError, ClientError) as e:
        print(f"Failed to download file from S3: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during file download: {e}")

