import pandas as pd
from cryptography.fernet import Fernet

keyDirectory = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\metadata\\secret.key'
scKey = open(keyDirectory, 'rb').read()
cipherSuite = Fernet(scKey)

def decryptData(data,cipherSuite):
    if isinstance(data,str):
        return cipherSuite.decrypt(data.encode()).decode()
    else:
        return data

'''
file_encrypt = "C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\metadata\\Meta_File_encrypt_python.txt"
df = pd.read_csv(file_encrypt)

df_decrypted = df.map(lambda x: decrypt_data(str(x),cipher_suite2))

print(df_decrypted)
'''
