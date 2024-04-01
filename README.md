# Logeagle

Logeagle is a Python tool for processing log data and converting it into Parquet format.

## Overview

Log files often contain valuable information, but they can be challenging to analyze efficiently due to their size and format. Logeagle simplifies this process by providing a convenient way to read log files, clean the data, and convert it into the Parquet format, which is optimized for analytics and storage.

## Features

- **Data Cleaning:** Logeagle cleans log data by removing unnecessary characters, formatting inconsistencies, and other noise.
- **Parquet Conversion:** It converts cleaned log data into the Parquet format, making it easy to analyze and store using various data processing tools.
- **Continuous Processing:** Logeagle can continuously process log data, ensuring that the Parquet files are always up-to-date with the latest information.

## Getting Started

To use Logeagle, follow these steps:

1. Clone the repository: `git clone https://github.com/logeagle/py.git`
2. Install dependencies: `pip install pyarrow`
3. Run Logeagle: `python logeagle.py`

## Usage

Logeagle accepts log files in various formats, such as Apache access logs, nginx error logs, and custom log formats. You can specify the input log files and output directory using command-line arguments.

Example:

```bash
python logeagle.py --input /path/to/logs/access.log --output /path/to/output

or

python logeagle.py 
```

For continuous processing, you can run Logeagle as a background process or utilize a task scheduler to execute it at regular intervals.

## Contributing

Contributions to Logeagle are welcome! If you have any ideas for improvements, bug fixes, or new features, feel free to open an issue or submit a pull request on GitHub.

## License

Logeagle is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for more information.
