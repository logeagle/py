#!/usr/bin/env python3

import os
import time
import random
from datetime import datetime
import pyarrow as pa
from pyarrow import schema as arrow_schema
from pyarrow.parquet import ParquetWriter
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class Config:
    def __init__(self):
        self.access_log = "/var/log/nginx/access.log"
        self.error_log = "/var/log/nginx/error.log"
        self.output_dir = os.path.expanduser("~/logeagle")
        self.rotation_interval = 3600  # 1 hour
        self.batch_size = 100  # Number of logs to accumulate before writing

        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, log_file: str, output_prefix: str, rotation_interval: int, log_type: str):
        self.log_file = log_file
        self.output_prefix = output_prefix
        self.last_position = 0
        self.schema = arrow_schema([
            ('timestamp', pa.timestamp('s')),
            ('line', pa.string())
        ])
        self.writer = None
        self.rotation_interval = rotation_interval
        self.last_rotation_time = time.time()
        self.log_type = log_type
        self.buffer = []

    def on_modified(self, event):
        if event.src_path == self.log_file:
            self.process_new_lines()

    def process_new_lines(self):
        try:
            if not os.path.exists(self.log_file):
                self.generate_sample_logs()
                return

            with open(self.log_file, 'r') as file:
                file.seek(self.last_position)
                for line in file:
                    self.buffer_line(line.strip())
                self.last_position = file.tell()

            # Write buffer if it's full
            if len(self.buffer) >= config.batch_size:
                self.flush_buffer()

        except Exception as e:
            print(f"Error processing {self.log_file}: {e}")

    def buffer_line(self, line: str):
        if line:
            current_time = int(time.time())
            self.buffer.append((current_time, line))

    def flush_buffer(self):
        if not self.buffer:
            return

        try:
            if self.writer is None or self._should_rotate():
                self._rotate_file()

            table = pa.Table.from_arrays(
                [
                    pa.array([row[0] for row in self.buffer], type=pa.timestamp('s')),
                    pa.array([row[1] for row in self.buffer], type=pa.string())
                ],
                schema=self.schema
            )

            self.writer.write_table(table)
            print(f"Wrote {len(self.buffer)} logs to {self.output_prefix}")
            self.buffer.clear()

        except Exception as e:
            print(f"Error writing to Parquet file: {e}")

    def generate_sample_logs(self):
        """Generate sample logs for testing when log files don't exist"""
        current_time = int(time.time())
        
        if self.log_type == 'access':
            ips = [f"192.168.1.{random.randint(1, 255)}" for _ in range(5)]
            paths = ["/", "/api", "/login", "/dashboard", "/static/main.css"]
            methods = ["GET", "POST", "PUT", "DELETE"]
            
            for _ in range(10):
                log = (
                    f'{current_time} - {random.choice(ips)} - - '
                    f'[{datetime.now().strftime("%d/%b/%Y:%H:%M:%S")} +0000] '
                    f'"{random.choice(methods)} {random.choice(paths)} HTTP/1.1" '
                    f'{random.choice([200, 201, 404, 500])} {random.randint(100, 5000)}'
                )
                self.buffer_line(log)
        else:
            levels = ["error", "warn", "notice"]
            messages = [
                "Connection refused",
                "File not found",
                "Invalid request",
                "Database timeout",
                "Memory limit exceeded"
            ]
            
            for _ in range(10):
                log = f'{current_time} - {random.choice(levels)} - {random.choice(messages)}'
                self.buffer_line(log)

        self.flush_buffer()

    def _should_rotate(self) -> bool:
        return time.time() - self.last_rotation_time >= self.rotation_interval

    def _rotate_file(self):
        if self.writer:
            self.writer.close()

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        new_output_file = f"{self.output_prefix}.{timestamp}.parquet"
        self.writer = ParquetWriter(new_output_file, self.schema)
        self.last_rotation_time = time.time()
        print(f"Rotated to new file: {new_output_file}")

def main():
    config = Config()
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

    # Schedule monitoring for both log files
    observer.schedule(access_handler, path=os.path.dirname(config.access_log), recursive=False)
    observer.schedule(error_handler, path=os.path.dirname(config.error_log), recursive=False)

    print(f"Starting log monitoring. Output directory: {config.output_dir}")
    observer.start()

    try:
        while True:
            time.sleep(10)  # Check for changes every 10 seconds
            access_handler.process_new_lines()
            error_handler.process_new_lines()
    except KeyboardInterrupt:
        print("\nStopping log monitoring...")
        observer.stop()
    finally:
        # Flush any remaining logs before shutting down
        access_handler.flush_buffer()
        error_handler.flush_buffer()
        observer.join()
        print("Log monitoring stopped.")

if __name__ == "__main__":
    main()