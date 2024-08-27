import pandas as pd
import os

def splitFile(input_file_path, output_dir, num_records, delimiter=','):
    """
    Split a big file into smaller files based on the input number of records.

    Args:
    - input_file_path (str): Path to the input file.
    - output_dir (str): Directory for the output files.
    - num_records (int): Number of records per output file.
    - delimiter (str): Delimiter used in the input and output files. Default is ','.
    """
    try:
        # Get the input file name from the input file path
        input_file_name = os.path.basename(input_file_path)

        # Split the input file name into name and extension
        input_file_name_no_ext, file_extension = os.path.splitext(input_file_name)

        header = pd.read_csv(input_file_path, dtype=str, nrows=0, sep=delimiter).columns.tolist()
        for i, chunk in enumerate(pd.read_csv(input_file_path, dtype=str, chunksize=num_records, sep=delimiter)):
            output_filename = f"{os.path.join(output_dir, input_file_name_no_ext)}_file{i}{file_extension}"
            chunk.to_csv(output_filename, index=False, header=header, sep=delimiter)
    except Exception as e:
        print(f"Error: {e}")

# Example usage
inputFilePath = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\processed\\LA_processed\\Hurley_Medical_8.2021-7.2023.csv'
outputDir = 'C:\\Users\\jvineet\\PycharmProjects\\PythonLearnings\\Data\\processed\\SplitFile'
numRecords = 10000
delimiter = '|'  # Specify the delimiter
splitFile(inputFilePath, outputDir, numRecords, delimiter)
