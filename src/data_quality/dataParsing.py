import os
import csv
from datetime import datetime

class delimiterValidator:
    """
    Validates rows in a delimiter-separated file and separates them into valid and invalid rows.
    """

    def __init__(self, input_file_path, expected_columns, delimiter, valid_output_file_path, rejected_output_file_path):
        """
        Initializes the DelimiterValidator with paths, expected column count, and delimiter.
        """
        self.input_file_path = input_file_path
        self.expected_columns = expected_columns
        self.delimiter = delimiter
        self.valid_output_file_path = valid_output_file_path
        self.rejected_output_file_path = rejected_output_file_path

        os.makedirs(self.valid_output_file_path, exist_ok=True)
        os.makedirs(self.rejected_output_file_path, exist_ok=True)

    def validate_row(self, row: str) -> bool:
        """
        Validates a single row by counting delimiters outside of quoted text.
        """
        reader = csv.reader([row], delimiter=self.delimiter, quotechar='"')
        try:
            row_data = next(reader)
        except StopIteration:
            return False

        return len(row_data) == self.expected_columns

    def process_file(self, input_file_name):
        """
        Processes the input file, separating valid and invalid rows into separate output files.
        Returns:
            tuple: (file_name, total_rows, valid_rows_count, invalid_rows_count, job_status)
        """
        valid_rows = []
        invalid_rows = []

        input_file = os.path.join(self.input_file_path, input_file_name)
        valid_output_file = os.path.join(self.valid_output_file_path, f"valid_{input_file_name}")
        rejected_output_file = os.path.join(self.rejected_output_file_path, f"rejected_{input_file_name}")

        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")

        total_rows = 0
        valid_rows_count = 0
        invalid_rows_count = 0
        job_status = "Success"

        try:
            with open(input_file, 'r') as file:
                for line in file:
                    total_rows += 1
                    if self.validate_row(line):
                        valid_rows.append(line)
                        valid_rows_count += 1
                    else:
                        invalid_rows.append(line)
                        invalid_rows_count += 1
        except IOError as e:
            job_status = "Failed"
            return (input_file_name, total_rows, valid_rows_count, invalid_rows_count, job_status)

        try:
            with open(valid_output_file, 'w') as valid_file:
                valid_file.writelines(valid_rows)
        except IOError as e:
            job_status = "Failed"
            return (input_file_name, total_rows, valid_rows_count, invalid_rows_count, job_status)

        try:
            with open(rejected_output_file, 'w') as rejected_file:
                rejected_file.writelines(invalid_rows)
        except IOError as e:
            job_status = "Failed"
            return (input_file_name, total_rows, valid_rows_count, invalid_rows_count, job_status)

        return (input_file_name, total_rows, valid_rows_count, invalid_rows_count, job_status)