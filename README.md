# Python Feed Tools

Python examples for downloading and processing spur feeds. All scripts were tested with python version 3.10.12.

## Scripts

There are two main scripts in this repository:

1. `parallel.py`: This script streams data from a feed URL and processes it in batches using a ProcessPoolExecutor. The data is streamed from the URL specified by the feed type, which can be either 'anonymous' or 'anonymous-residential'. The function uses a maximum of 4 workers to process the batches in parallel. The total number of lines processed is printed at the end.

2. `serial.py`: This script streams data from a feed URL and processes it line by line. The data is streamed from the URL specified by the feed type, which can be either 'anonymous' or 'anonymous-residential'. Each line is assumed to be a JSON object. The processing of a line involves parsing it as JSON. The processed line is returned.

## Usage

To use these scripts, you need to set the `API_TOKEN` environment variable to your API token. Then, you can run the scripts with Python 3.10.12.

For `parallel.py`:

```bash
python parallel.py
```

For `serial.py`:

```bash
python serial.py
```

Please note that the actual processing (e.g., inserting into a database) is not implemented in the current version of the scripts.