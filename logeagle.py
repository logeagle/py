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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("log_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    username: str = pwd.getpwuid(os.getuid())[0]
    access_log: str = f"/var/log/nginx/access.log"
    error_log: str = f"/var/log/nginx/error.log"
    output_dir: str = f"/home/{username}/logeagle"
    chunk_size: int = 1000
    rotation_interval: int = 3600  # 1 hour

config = Config()

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, log_file: str, output_file: str, chunk_size: int, rotation_interval: int):
        self.log_file = log_file
        self.output_file = output_file
        self.last_position = 0
        self.schema = arrow_schema([('timestamp', pa.timestamp('s')), ('line', pa.string())])
        self.writer = None
        self.chunk_size = chunk_size
        self.rotation_interval = rotation_interval
        self.last_rotation_time = time.time()
        self.buffer = []

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
                self.buffer.extend(new_lines)
                if len(self.buffer) >= self.chunk_size or self._should_rotate():
                    self.write_to_parquet()
        except Exception as e:
            logger.error(f"Error processing file {self.log_file}: {str(e)}")

    def write_to_parquet(self):
        if not self.buffer:
            return

        try:
            current_time = int(time.time())
            cleaned_data = [(current_time, line.strip()) for line in self.buffer if line.strip()]

            table = pa.Table.from_arrays(
                [pa.array([t[0] for t in cleaned_data], type=pa.timestamp('s')),
                 pa.array([t[1] for t in cleaned_data], type=pa.string())],
                schema=self.schema
            )

            if self.writer is None or self._should_rotate():
                if self.writer:
                    self.writer.close()
                self._rotate_file()

            self.writer.write_table(table)
            logger.info(f"Wrote {len(cleaned_data)} new lines to {self.output_file}")
            self.buffer.clear()
        except Exception as e:
            logger.error(f"Error writing to Parquet file {self.output_file}: {str(e)}")

    def _should_rotate(self) -> bool:
        return time.time() - self.last_rotation_time >= self.rotation_interval

    def _rotate_file(self):
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        new_output_file = f"{self.output_file}.{timestamp}"
        self.writer = ParquetWriter(new_output_file, self.schema)
        self.last_rotation_time = time.time()
        logger.info(f"Rotated to new file: {new_output_file}")

def main(debug: bool = False):
    """Main function to run the log processor."""
    os.makedirs(config.output_dir, exist_ok=True)

    observer = Observer()

    access_handler = LogFileHandler(
        config.access_log, 
        os.path.join(config.output_dir, "access.parquet"),
        config.chunk_size,
        config.rotation_interval
    )
    observer.schedule(access_handler, path=os.path.dirname(config.access_log), recursive=False)

    error_handler = LogFileHandler(
        config.error_log, 
        os.path.join(config.output_dir, "error.parquet"),
        config.chunk_size,
        config.rotation_interval
    )
    observer.schedule(error_handler, path=os.path.dirname(config.error_log), recursive=False)

    observer.start()
    logger.info("Started monitoring log files.")

    try:
        while True:
            time.sleep(1)
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