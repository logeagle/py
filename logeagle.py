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
from typing import List, Tuple
import random

# Configuration using a dataclass
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

# Set up logging with DEBUG level for detailed output
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.output_dir, "log_processor.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, log_file: str, output_prefix: str, rotation_interval: int, log_type: str):
        self.log_file = log_file
        self.output_prefix = output_prefix
        self.rotation_interval = rotation_interval
        self.log_type = log_type
        self.buffer: List[Tuple[int, str]] = []  # Buffer to collect log rows
        self.last_position = 0
        self.last_rotation_time = time.time()
        self.schema = arrow_schema([('timestamp', pa.timestamp('s')), ('line', pa.string())])
        self.writer = None

    def on_modified(self, event):
        """Handle file modifications."""
        if event.src_path == self.log_file:
            logger.debug(f"Detected modification in {self.log_file}")
            self.process_new_lines()

    def process_new_lines(self):
        """Process and store new lines from the log file."""
        try:
            with open(self.log_file, 'r') as file:
                file.seek(self.last_position)  # Start from the last read position
                lines = file.readlines()
                if lines:
                    for line in lines:
                        self.write_to_buffer(line.strip())
                    self.last_position = file.tell()  # Update the position
                else:
                    logger.warning(f"No new lines in {self.log_file}. Generating sample logs.")
                    self.generate_sample_logs()
        except Exception as e:
            logger.error(f"Error processing file {self.log_file}: {e}")

    def generate_sample_logs(self):
        """Generate and process sample logs for testing."""
        current_time = int(time.time())
        if self.log_type == 'access':
            sample_logs = [
                f'{current_time} - 192.168.1.{random.randint(1, 255)} - "GET /page{random.randint(1, 10)}.html HTTP/1.1" 200 {random.randint(1000, 5000)}',
                f'{current_time} - 10.0.0.{random.randint(1, 255)} - "POST /api/data HTTP/1.1" 201 {random.randint(100, 1000)}'
            ]
        else:  # error logs
            sample_logs = [
                f'{current_time} - error - Database connection failed.',
                f'{current_time} - warn - High CPU usage detected.'
            ]
        for log in sample_logs:
            self.write_to_buffer(log)

    def write_to_buffer(self, line: str):
        """Add log lines to the buffer and write them to Parquet when needed."""
        current_time = int(time.time())
        self.buffer.append((current_time, line))

        if len(self.buffer) >= 100 or self._should_rotate():
            self.write_to_parquet()

    def write_to_parquet(self):
        """Write buffered logs to a Parquet file."""
        try:
            if self.writer is None or self._should_rotate():
                self._rotate_file()

            table = pa.Table.from_arrays(
                [pa.array([row[0] for row in self.buffer], type=pa.timestamp('s')),
                 pa.array([row[1] for row in self.buffer], type=pa.string())],
                schema=self.schema
            )
            self.writer.write_table(table)
            logger.info(f"Wrote {len(self.buffer)} rows to {self.output_prefix}")
            self.buffer.clear()  # Clear the buffer
        except Exception as e:
            logger.error(f"Error writing to Parquet: {e}")

    def _should_rotate(self) -> bool:
        """Check if it's time to rotate the log file."""
        return time.time() - self.last_rotation_time >= self.rotation_interval

    def _rotate_file(self):
        """Rotate the output Parquet file."""
        if self.writer:
            self.writer.close()

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        new_file = f"{self.output_prefix}.{timestamp}.parquet"
        self.writer = ParquetWriter(new_file, self.schema)
        self.last_rotation_time = time.time()
        logger.info(f"Rotated to new file: {new_file}")

def main(debug: bool = False):
    """Main function to start log monitoring."""
    observer = Observer()

    # Set up handlers for both access and error logs
    access_handler = LogFileHandler(
        config.access_log,
        os.path.join(config.output_dir, "access"),
        config.rotation_interval,
        'access'
    )
    error_handler = LogFileHandler(
        config.error_log,
        os.path.join(config.output_dir, "error"),
        config.rotation_interval,
        'error'
    )

    # Schedule the handlers
    observer.schedule(access_handler, path=os.path.dirname(config.access_log), recursive=False)
    observer.schedule(error_handler, path=os.path.dirname(config.error_log), recursive=False)

    observer.start()
    logger.info(f"Started monitoring log files in real-time. Output directory: {config.output_dir}")

    try:
        while True:
            time.sleep(10)  # Keep checking every 10 seconds
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
        logger.error(f"An error occurred: {e}")
        exit(1)
