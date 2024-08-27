
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime, insert, select
from sqlalchemy.orm import sessionmaker
import pandas as pd
def readMetadata(config):
    """
    Reads the metadata from the MySQL database.

    Args:
        config (dict): A dictionary containing the database configuration.
            - user (str): The database user.
            - password (str): The database password.
            - host (str): The database host.
            - db (str): The database name.

    Returns:
        db_connection (Engine): The database connection.
        session (Session): The database session.
        s3_files_table (Table): The s3_files table.
        aws_access_key_id (str): The AWS access key ID.
        aws_secret_access_key (str): The AWS secret access key.
    """
    # Connect to MySQL
    dbUrl = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
    db_connection = create_engine(dbUrl)

    # Define the metadata
    metadata = MetaData()
    metadata.reflect(bind=db_connection)

    # Create a session
    Session = sessionmaker(bind=db_connection)
    session = Session()

    # Define the table
    s3_files_table = Table('s3_files', metadata, autoload_with=db_connection)

    # Query to get AWS credentials
    query = "SELECT * FROM joblog_metadata.prod_l1_context_db;"
    df_mysql = pd.read_sql(query, db_connection)
    keys_dict = df_mysql.set_index('key')['value'].to_dict()
    aws_access_key_id = keys_dict['S3_AccessKey']
    aws_secret_access_key = keys_dict['S3_Secret_Access_Key']

    # Return the metadata
    return (
        db_connection,
        session,
        s3_files_table,
        aws_access_key_id,
        aws_secret_access_key
    )

# Example usage
