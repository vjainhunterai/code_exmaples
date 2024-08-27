from datetime import datetime
import os
import uuid
import boto3
from botocore.client import BaseClient
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, MetaData, insert, select
from sqlalchemy.orm import sessionmaker

from src.utils import (
    readEncryptedConfig,
    readMetadata,
    databaseHandler,
    stepLogger,
    auditLogger

)
from src.etl import (
    getS3Directories,
    listFilesInS3Directories,
    getIncrementalFiles,
    insertFilesToDb,
    fetchUnprocessedFiles,
    downloadAndProcessFile,
    fileMover,

)
from src.data_quality import (
    FileSizeProcessor,
    delimiterValidator,
    schemaValidator
)

if __name__ == '__main__':
    # Initialize configuration and metadata

    relativeExcelPath = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\metadata\\paths.xlsx'
    config = readEncryptedConfig(relativeExcelPath)
    #print(config)

    # Database connection
    host = config['host']
    db = config['database']
    user = config['user']
    password = config['password']
    dbUrl = f"mysql+pymysql://{user}:{password}@{host}/{db}"
    port = 3306
    dbConnection, session, s3FilesTable, awsAccessKeyId, awsSecretAccessKey = readMetadata(config)

    regionName = 'us-east-1'
    bucketName = 'etlhunter'
    daysS3 = 25

    # Create S3 client
    s3Client: BaseClient = boto3.client('s3', aws_access_key_id=awsAccessKeyId, aws_secret_access_key=awsSecretAccessKey, region_name=regionName)

    # Initialize loggers
    dbHandler = databaseHandler(dbUrl)
    step_logger = stepLogger(dbHandler)
    audit_logger = auditLogger(dbHandler)

    # Generate a unique execution ID
    exec_id = str(uuid.uuid4())

    # Get S3 directories and list files
    listDir = getS3Directories(daysS3)
    print(listDir)
    s3Files = listFilesInS3Directories(listDir, s3Client, bucketName)

    # Get all file names from audit table and find incremental files
    auditFiles_query = 'SELECT DISTINCT file_name, s3_last_modified_date FROM joblog_metadata.L1_audit_data'
    auditFiles = pd.read_sql(auditFiles_query, dbConnection)
    incrementalFilesDf = getIncrementalFiles(s3Files, auditFiles)

    # Insert incremental files into the database
    insertFilesToDb(incrementalFilesDf, s3FilesTable, session, insert)

    # Fetch unprocessed files and process them
    fetchUnprocessedFile = fetchUnprocessedFiles(s3FilesTable, session, select)
    localDirectoryLA = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\raw\\LA'
    processed_files = set()

    if isinstance(listDir, list):
        for directory in listDir:
            for file in fetchUnprocessedFile:
                file_identifier = file['File_Name']  # Adjust this key based on your actual file structure

                if file_identifier not in processed_files:
                    start_time = datetime.now()
                    try:
                        downloadAndProcessFile(s3Client, bucketName, file, directory, localDirectoryLA)
                        processed_files.add(file_identifier)

                        # File size check after downloading
                        filePath = os.path.join(localDirectoryLA, file_identifier) # Adjust the path if necessary

                        processor = FileSizeProcessor(localDirectoryLA)
                        result = processor.processFile()




                        if result[0] == 0 :  # Assuming processFile returns "File is empty" if the file is empty
                            audit_entry = {
                                'exec_id': exec_id,
                                'job_name': 'L1 Data Load',
                                'file_name': file_identifier,
                                'file_date': datetime.now(),
                                'total_rec_cnt': 0,
                                'processed_cnt': 0,
                                'job_status': 'Failed',  # Mark status as Failed
                                'rejection_cnt': 0,
                                'rejection_rsn': 'File is empty',
                                's3_last_modified_date': datetime.now()
                            }
                            end_time = datetime.now()
                            step_logger.logStep(
                                execId=exec_id,  # Example exec_id, replace with your actual value
                                jobName="File Size Check",
                                fileName=file_identifier,
                                startTime=start_time,
                                endTime=end_time,
                                srcCnt=0,
                                tgtCnt=0,
                                lkpCnt=0,
                                rejCnt=0,
                                jobStatus="Failed"
                            )
                        else:
                            # Count the number of records in the file
                            with open(filePath, 'r') as f:
                                src_cnt = sum(1 for line in f) - 1  # Subtract 1 to ignore the header row

                            audit_entry = {
                                'exec_id': exec_id,
                                'job_name': 'L1 Data Load',
                                'file_name': file_identifier,
                                'file_date': datetime.now(),
                                'total_rec_cnt': src_cnt,
                                'processed_cnt': 0,
                                'job_status': 'WIP',  # Mark status as WIP
                                'rejection_cnt': 0,
                                'rejection_rsn': '',
                                's3_last_modified_date': datetime.now()
                            }
                            end_time = datetime.now()
                            step_logger.logStep(
                                execId=exec_id,  # Example exec_id, replace with your actual value
                                jobName="File Size Check",
                                fileName=file_identifier,
                                startTime=start_time,
                                endTime=end_time,
                                srcCnt=src_cnt,
                                tgtCnt=0,
                                lkpCnt=0,
                                rejCnt=0,
                                jobStatus="Success"
                            )

                        # Log the audit entry
                        audit_logger.logAudit(audit_entry)

                    except Exception as e:
                        # Log the failure in the audit entry
                        audit_entry = {
                            'exec_id': exec_id,
                            'job_name': 'L1 Data Load',
                            'file_name': file_identifier,
                            'file_date': datetime.now(),
                            'total_rec_cnt': 0,
                            'processed_cnt': 0,
                            'job_status': 'Failed',  # Mark status as Failed due to error
                            'rejection_cnt': 0,
                            'rejection_rsn': f'Error: {e}',
                            's3_last_modified_date': datetime.now()
                        }
                        end_time = datetime.now()
                        step_logger.logStep(
                            execId=exec_id,  # Example exec_id, replace with your actual value
                            jobName="File Size Check",
                            fileName=file_identifier,
                            startTime=start_time,
                            endTime=end_time,
                            srcCnt=0,
                            tgtCnt=0,
                            lkpCnt=0,
                            rejCnt=0,
                            jobStatus="Failed"
                        )
                        audit_logger.logAudit(audit_entry)
    # Move files to respective folders
    source_directory = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\raw\\LA'
    target_base_directory = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\processed\\processed_source'
    fileMover = fileMover(source_directory, target_base_directory)
    fileMover.move_files()

    # Validate schema for each file
    dbUrl1 = "mysql+pymysql://admin:Gpoproddb!#!@prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com/joblog_metadata"
    tableName = 'AP'
    schemaTable = 'Table_Schema'
    mappingTable = 'mapping_table'
    fileType = 'AP'
    directory_path = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\processed\\processed_source\\AP'
    target_directory = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\processed\\processed_schema_valid\\AP'
    rejection_directory = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\processed\\processed_rejection\\AP'
    delimiter = '|'

    schema_validator = schemaValidator(dbUrl1)
    start_time = datetime.now()
    results, total_source_record_count, total_target_record_count = schema_validator.validate_directory(
        schemaTable, tableName, directory_path, delimiter, mappingTable, fileType, target_directory, rejection_directory
    )
    end_time = datetime.now()

    for file, status, record_count in results:
        if status == 1:
            job_status = "Success"
            tgt_cnt = record_count
            rej_cnt = 0
        elif status == 0:
            job_status = "Failed"
            tgt_cnt = 0
            rej_cnt = record_count
        elif status == -1:
            job_status = "Error"
            tgt_cnt = 0
            rej_cnt = 0
        else:
            job_status = "Skipped"
            tgt_cnt = 0
            rej_cnt = 0

        step_logger.logStep(
            execId=exec_id,
            jobName="Schema Validation",
            fileName=file,
            startTime=start_time,
            endTime=end_time,
            srcCnt=total_source_record_count,
            tgtCnt=tgt_cnt,
            lkpCnt=0,
            rejCnt=rej_cnt,
            jobStatus=job_status
        )

    # Clean up
    #dbConnection.dispose()
    # Example usage with step_logger integration
    try:
        # Initialize the validator
        validator = delimiterValidator(
            input_file_path='C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\processed\\processed_schema_valid\\AP',
            expected_columns=31,
            delimiter=delimiter,
            valid_output_file_path='C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\processed\\processed_valid\\AP',
            rejected_output_file_path='C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\processed\\processed_rejection\\AP'
        )

        # Track execution start time
        start_time = datetime.now()

        # Get a list of all files in the input directory
        files = [f for f in os.listdir(validator.input_file_path) if
                 os.path.isfile(os.path.join(validator.input_file_path, f))]

        total_source_record_count = 0
        total_target_record_count = 0

        # Process each file and log the results
        results = []
        for file in files:
            file_name, total_rows, valid_rows_count, invalid_rows_count, job_status = validator.process_file(file)
            total_source_record_count += total_rows
            total_target_record_count += valid_rows_count
            results.append((file_name, job_status, valid_rows_count if job_status == "Success" else invalid_rows_count))

        # Track execution end time
        end_time = datetime.now()

        # Log each file's status using step_logger.logStep
        for file, status, record_count in results:
            if status == "Success":
                job_status = "Success"
                tgt_cnt = record_count
                rej_cnt = 0
            elif status == "Failed":
                job_status = "Failed"
                tgt_cnt = 0
                rej_cnt = record_count
            else:
                job_status = "Skipped"
                tgt_cnt = 0
                rej_cnt = 0

            # Logging the step using step_logger.logStep
            step_logger.logStep(
                execId=exec_id,
                jobName="Delimiter Validation",
                fileName=file,
                startTime=start_time,
                endTime=end_time,
                srcCnt=total_source_record_count,
                tgtCnt=tgt_cnt,
                lkpCnt=0,
                rejCnt=rej_cnt,
                jobStatus=job_status
            )

        # Clean up any resources if necessary (e.g., closing connections)

    except Exception as e:
        # Handle the exception (you might want to log this in step_logger as well)
        print(f"An error occurred: {e}")


