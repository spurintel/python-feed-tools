"""
This module contains functions for streaming and processing data sequentially.

Functions:
    process_line(line): Processes a single line of data, assuming the line is a JSON object.
    main(): Main function to stream and process data sequentially.

Modules:
    argparse: Used to parse command line arguments.
    requests: Used to make HTTP requests.
    gzip: Used to handle gzip compressed files.
    json: Used to parse JSON data.
    time: Used to measure elapsed time.
    os: Used to get the API token from an environment variable.

The script uses the requests module to stream data from a specified URL and processes it line by line. 
The data is streamed from the URL specified by the feed type, which can be either 'anonymous' or 'anonymous-residential'. 
The function uses the API token set in the environment variable API_TOKEN. 
Each line is assumed to be a JSON object. The processing of a line involves parsing it as JSON. 
The processed line is returned.

Note: The actual processing (e.g., inserting into a database) is not implemented in the current version of the script.
"""

import argparse
import gzip
import json
import time
import os
import requests

def process_line(line):
    """
    Process a single line of data.

    This function assumes each line is a JSON-formatted string.
    It parses the JSON data and can be modified to perform additional processing.

    Args:
        line (str): A string representing a single line of data.

    Returns:
        str: The processed line.
    """
    parsed = json.loads(line)
    # TODO: Perform processing here (e.g., data transformation, database insertion, etc.)
    return line

def main():
    """
    Main function to stream and process data sequentially.

    This function streams data from a specified URL and processes it line by line. 
    The data is streamed from the URL specified by the feed type, which can be either 'anonymous' 
    or 'anonymous-residential'. The function uses the API token set in the environment variable API_TOKEN.
    Each line is assumed to be a JSON object and is processed by the process_line function. 
    The total number of lines processed and the elapsed time are printed at the end.

    Environment Variables:
        API_TOKEN: The token to include in the request headers.

    Returns:
        None
    """
    start_time = time.time()

    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Stream and process data from a specified feed type.")
    parser.add_argument("--feed_type", default="anonymous", choices=["anonymous", "anonymous-residential"],
                        help="Type of feed to process (default: %(default)s)")
    args = parser.parse_args()

    # Determine the URL based on the feed type
    base_url = 'https://feeds.spur.us/v2/'
    if args.feed_type == "anonymous":
        url = base_url + 'anonymous/latest.json.gz'
    elif args.feed_type == "anonymous-residential":
        url = base_url + 'anonymous-residential/latest.json.gz'

    token = os.getenv('API_TOKEN', '')  # Get the token from an environment variable
    if not token or token == '':
        print("Please set the API_TOKEN environment variable")
        return

    headers = {'TOKEN': token}
    response = requests.get(url, headers=headers, stream=True)

    line_count = 0

    if response.status_code == 200:
        with gzip.GzipFile(fileobj=response.raw) as gzip_file:
            for line in gzip_file:
                processed_line = process_line(line.decode().strip())
                # Increment line count
                line_count += 1

    else:
        print(f"Failed to retrieve data: {response.status_code}")

    elapsed_time = time.time() - start_time
    print(f"Total time taken: {elapsed_time:.2f} seconds")
    print(f"Total lines processed: {line_count}")

if __name__ == "__main__":
    main()
