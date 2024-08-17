import os
import pwd
import pyarrow as pa
from pyarrow import schema as arrow_schema
from pyarrow.parquet import ParquetWriter
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, log_file, output_file):
        self.log_file = log_file
        self.output_file = output_file
        self.last_position = 0
        self.schema = arrow_schema([('timestamp', pa.timestamp('s')), ('line', pa.string())])
        self.writer = None

    def on_modified(self, event):
        if event.src_path == self.log_file:
            self.process_new_lines()

    def process_new_lines(self):
        try:
            with open(self.log_file, 'r') as file:
                file.seek(self.last_position)
                new_lines = file.readlines()
                self.last_position = file.tell()

            if new_lines:
                self.write_to_parquet(new_lines)
        except Exception as e:
            logger.error(f"Error processing file {self.log_file}: {str(e)}")

    def write_to_parquet(self, data):
        try:
            # Remove null values and prepare data
            current_time = int(time.time())
            cleaned_data = [(current_time, line.strip()) for line in data if line.strip()]

            # Create PyArrow table
            table = pa.Table.from_arrays(
                [pa.array([t[0] for t in cleaned_data], type=pa.timestamp('s')),
                 pa.array([t[1] for t in cleaned_data], type=pa.string())],
                schema=self.schema
            )

            # Write table to Parquet file
            if self.writer is None:
                self.writer = ParquetWriter(self.output_file, self.schema)
            
            self.writer.write_table(table)
            logger.info(f"Wrote {len(cleaned_data)} new lines to {self.output_file}")
        except Exception as e:
            logger.error(f"Error writing to Parquet file {self.output_file}: {str(e)}")

def get_username():
    return pwd.getpwuid(os.getuid())[0]

def main(access_log_path, error_log_path, output_directory):
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Set up observers for both log files
    observer = Observer()
    
    access_handler = LogFileHandler(access_log_path, os.path.join(output_directory, "access.parquet"))
    observer.schedule(access_handler, path=os.path.dirname(access_log_path), recursive=False)
    
    error_handler = LogFileHandler(error_log_path, os.path.join(output_directory, "error.parquet"))
    observer.schedule(error_handler, path=os.path.dirname(error_log_path), recursive=False)

    observer.start()
    logger.info("Started monitoring log files.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-time log processor")
    parser.add_argument("--access-log", default="/var/log/nginx/access.log", help="Path to access log file")
    parser.add_argument("--error-log", default="/var/log/nginx/error.log", help="Path to error log file")
    parser.add_argument("--output-dir", default=f"/home/{get_username()}/logeagle", help="Output directory for Parquet files")
    
    args = parser.parse_args()

    main(args.access_log, args.error_log, args.output_dir)