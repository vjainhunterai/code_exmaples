from src.utils import decryptData
import pandas as pd
from cryptography.fernet import Fernet
import os

def readEncryptedConfig(excelFilePath):
    """
    Reads the encrypted configuration from the Excel file.

    Args:
        excel_file_path (str): The path to the Excel file.

    Returns:
        dict: A dictionary containing the decrypted configuration.
    """
    # Read paths from Excel file
    pathsDf = pd.read_excel(excelFilePath)
    pathsDict = pathsDf.set_index('Key_name')['Path'].to_dict()

    # Read the encryption key
    keyPath = pathsDict['key_path']
    encryptedFile = pathsDict['encrypted_file']

    # Read the encryption key
    scKey = open(keyPath, 'rb').read()
    cipherSuite = Fernet(scKey)

    # Read the encrypted file
    df = pd.read_csv(encryptedFile)

    # Decrypt the data
    df_decrypted = df.map(lambda x: decryptData(str(x), cipherSuite))
    df = pd.DataFrame(df_decrypted)

    # Extract configuration
    config = {
        'host': str(df.at[0, 'host']),
        'database': str(df.at[0, 'database']),
        'user': str(df.at[0, 'user']),
        'password': str(df.at[0, 'password']),
        'port': 3306
    }

    return config

# Example usage
