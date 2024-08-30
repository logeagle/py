#!/usr/bin/env python3

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
from dataclasses import dataclass
from typing import Dict, Any
import random

# Configuration
@dataclass
class Config:
    username: str = pwd.getpwuid(os.getuid())[0]
    access_log: str = "/var/log/nginx/access.log"
    error_log: str = "/var/log/nginx/error.log"
    output_dir: str = os.path.expanduser("~/logeagle")
    rotation_interval: int = 3600  # 1 hour

config = Config()

# Ensure the output directory exists
os.makedirs(config.output_dir, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logging
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.output_dir, "log_processor.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, log_file: str, output_file: str, rotation_interval: int, log_type: str):
        self.log_file = log_file
        self.output_file = output_file
        self.last_position = 0
        self.schema = arrow_schema([('timestamp', pa.timestamp('s')), ('line', pa.string())])
        self.writer = None
        self.rotation_interval = rotation_interval
        self.last_rotation_time = time.time()
        self.log_type = log_type
        self.buffer = []  # Add a buffer to collect rows

    def on_modified(self, event):
        if event.src_path == self.log_file:
            self.process_new_lines()

    def process_new_lines(self):
        try:
            if not os.path.exists(self.log_file):
                logger.warning(f"Log file {self.log_file} does not exist. Generating sample logs.")
                self.generate_sample_logs()
                return

            with open(self.log_file, 'r') as file:
                file.seek(self.last_position)
                lines = file.readlines()
                if not lines:
                    logger.warning(f"No new lines in {self.log_file}. Generating sample logs.")
                    self.generate_sample_logs()
                else:
                    for line in lines:
                        self.write_to_parquet(line.strip())
                self.last_position = file.tell()
        except Exception as e:
            logger.error(f"Error processing file {self.log_file}: {str(e)}")

    def generate_sample_logs(self):
        current_time = int(time.time())
        if self.log_type == 'access':
            sample_logs = [
                f'{current_time} - 192.168.1.{random.randint(1, 255)} - - [30/Aug/2024:{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d} +0000] "GET /page{random.randint(1, 10)}.html HTTP/1.1" 200 {random.randint(1000, 5000)}',
                f'{current_time} - 10.0.0.{random.randint(1, 255)} - - [30/Aug/2024:{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d} +0000] "POST /api/data HTTP/1.1" 201 {random.randint(100, 1000)}',
                f'{current_time} - 172.16.0.{random.randint(1, 255)} - - [30/Aug/2024:{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d} +0000] "GET /images/logo.png HTTP/1.1" 304 0'
            ]
        else:  # error logs
            sample_logs = [
                f'{current_time} - {random.choice(["error", "crit", "alert"])} - Error processing request: {random.choice(["File not found", "Database connection failed", "Invalid input"])}',
                f'{current_time} - warn - Warning: {random.choice(["High CPU usage", "Low disk space", "Slow database query"])}',
                f'{current_time} - notice - Notice: {random.choice(["Cache cleared", "Scheduled maintenance starting", "Config file updated"])}'
            ]
        for log in sample_logs:
            self.write_to_parquet(log)

    def write_to_parquet(self, line: str):
        if not line:
            return

        try:
            current_time = int(time.time())
            self.buffer.append((current_time, line))

            if len(self.buffer) >= 100 or self._should_rotate():  # Write in batches of 100 or when rotating
                if self.writer is None or self._should_rotate():
                    if self.writer:
                        self.writer.close()
                    self._rotate_file()

                table = pa.Table.from_arrays(
                    [pa.array([row[0] for row in self.buffer], type=pa.timestamp('s')),
                     pa.array([row[1] for row in self.buffer], type=pa.string())],
                    schema=self.schema
                )

                self.writer.write_table(table)
                logger.info(f"Wrote {len(self.buffer)} rows to {self.output_file}")
                self.buffer.clear()  # Clear the buffer after writing
        except Exception as e:
            logger.error(f"Error writing to Parquet file {self.output_file}: {str(e)}")

    def _should_rotate(self) -> bool:
        return time.time() - self.last_rotation_time >= self.rotation_interval

    def _rotate_file(self):
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        new_output_file = f"{self.output_file}.{timestamp}.parquet"
        self.writer = ParquetWriter(new_output_file, self.schema)
        self.last_rotation_time = time.time()
        logger.info(f"Rotated to new file: {new_output_file}")

def main(debug: bool = False):
    """Main function to run the log processor."""
    observer = Observer()

    access_handler = LogFileHandler(
        config.access_log, 
        os.path.join(config.output_dir, "access"),
        config.rotation_interval,
        'access'
    )
    observer.schedule(access_handler, path=os.path.dirname(config.access_log), recursive=False)

    error_handler = LogFileHandler(
        config.error_log, 
        os.path.join(config.output_dir, "error"),
        config.rotation_interval,
        'error'
    )
    observer.schedule(error_handler, path=os.path.dirname(config.error_log), recursive=False)

    observer.start()
    logger.info(f"Started monitoring log files in real-time. Output directory: {config.output_dir}")

    try:
        while True:
            time.sleep(10)  # Check for changes every 10 seconds
            access_handler.process_new_lines()
            error_handler.process_new_lines()
    except KeyboardInterrupt:
        logger.info("Stopping log monitoring...")
        observer.stop()
    finally:
        observer.join()
        logger.info("Log monitoring stopped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-time log processor")
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    args = parser.parse_args()

    try:
        main(debug=args.debug)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        exit(1)