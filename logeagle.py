# This Python code performs the same operations as the Rust code. It reads the log files, creates the output directory if it doesn't exist, and writes the data to Parquet files. The username is obtained using the whoami command via subprocess. Ensure you have the parquet library installed (pip install parquet).


import os
import subprocess
import shutil
from parquet import ParquetFile, schema
from parquet import write_simple
from datetime import datetime

def read_log_file(file_path):
    with open(file_path, 'r') as file:
        return file.readlines()

def write_to_parquet_file(file_path, data):
    with ParquetFile(file_path, 'w') as pf:
        pf.schema = schema([("line", "UTF8")])
        write_simple(pf, data)

def main():
    # Get username using whoami command
    username = subprocess.check_output(['whoami']).decode().strip()

    # Define paths for log files and output directory
    access_log_path = "/var/log/nginx/access.log"
    error_log_path = "/var/log/nginx/error.log"
    output_directory = f"/home/{username}/logeagle"

    # Read data from log files
    access_log_data = read_log_file(access_log_path)
    error_log_data = read_log_file(error_log_path)

    # Create output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Write data to Parquet files
    write_to_parquet_file(os.path.join(output_directory, "access.parquet"), access_log_data)
    write_to_parquet_file(os.path.join(output_directory, "error.parquet"), error_log_data)

    print("Data has been successfully converted and saved to Parquet format.")

if __name__ == "__main__":
    main()
