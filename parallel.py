"""
This module contains functions for streaming and processing data in parallel.

Functions:
    process_batch(batch): Processes a batch of lines, assuming each line is a JSON object.
    stream_and_process(url, token, batch_size=100000): Streams data from a given URL and processes it in batches.

Modules:
    argparse: Used to parse command line arguments.
    requests: Used to make HTTP requests.
    gzip: Used to handle gzip compressed files.
    json: Used to parse JSON data.
    time: Used to measure elapsed time.
    os: Used to get the API token from an environment variable.
    concurrent.futures: Used for parallel processing.

The script uses ProcessPoolExecutor from the concurrent.futures module to process data in parallel. 
The data is streamed from the URL specified by the feed type, which can be either 'anonymous' or 'anonymous-residential'. 
The data is streamed from a given URL and processed in batches. Each batch is a list of lines,
and each line is assumed to be a JSON object. The processing of a batch involves parsing each 
line as JSON.  The number of lines processed is returned.

Note: The actual processing is just a dummy operation. 
The script can be modified to perform the actual processing, 
such as inserting the data into a database.
"""

import argparse
import gzip
import json
import time
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import requests

def process_batch(batch):
    """
    Processes a batch of lines, assuming each line is a JSON object.

    This function parses each line in the batch as a JSON object. The actual processing 
    (e.g., inserting into a database) is not implemented in the current version of the script. 
    The function returns the number of lines processed.

    Args:
        batch (list): A list of lines, where each line is a string representing a JSON object.

    Returns:
        int: The number of lines processed.
    """
    for line in batch:
        parsed = json.loads(line)
        # TODO: Perform processing here (e.g., data transformation, database insertion, etc.)
    return len(batch)  # Return the count of lines processed


def stream_and_process(url, token, batch_size=100000):
    """
    Streams data from a given URL and processes it in batches.

    This function sends a GET request to the provided URL with the provided token in the headers. 
    If the response status code is not 200, it prints an error message and returns. 
    Otherwise, it reads the response in batches of the specified size, assuming the response is a gzip compressed file. 
    Each line in the response is decoded and stripped of leading/trailing whitespace. 
    When a batch reaches the specified size, it is yielded for processing. 
    Any remaining lines after the last full batch are also yielded.

    Args:
        url (str): The URL to stream data from.
        token (str): The token to include in the request headers.
        batch_size (int, optional): The number of lines per batch. Defaults to 100000.

    Yields:
        list: A batch of lines from the response.
    """
    headers = {'TOKEN': token}
    response = requests.get(url, headers=headers, stream=True)

    if response.status_code != 200:
        print(f"Failed to retrieve data: {response.status_code}")
        return

    batch = []
    with gzip.GzipFile(fileobj=response.raw) as gzip_file:
        for line in gzip_file:
            batch.append(line.decode().strip())
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def main():
    """
    Main function to stream and process data in parallel.

    This function streams data from a specified URL and processes it in batches using a ProcessPoolExecutor. 
    The token is retrieved from an environment variable. 
    The function uses a maximum of 4 workers to process the batches in parallel. 
    The total number of lines processed is printed at the end.

    The number of workers can be adjusted based on the system's capabilities.

    Environment Variables:
        API_TOKEN: The token to include in the request headers.

    Returns:
        None
    """
    start_time = time.time()

    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Stream and process data for a specified feed type.")
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

    max_workers = 4
    futures = set()
    total_lines_processed = 0

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit initial batch of futures
        for batch in stream_and_process(url, token):
            if len(futures) >= max_workers:
                # Wait for at least one future to complete
                for completed_future in as_completed(futures):
                    total_lines_processed += completed_future.result()
                    futures.remove(completed_future)
                    break  # Break after processing one completed future

            future = executor.submit(process_batch, batch)
            futures.add(future)

        # Wait for the remaining futures to complete
        for future in as_completed(futures):
            total_lines_processed += future.result()

    elapsed_time = time.time() - start_time
    print(f"Total time taken: {elapsed_time:.2f} seconds")
    print(f"Total lines processed: {total_lines_processed}")


if __name__ == "__main__":
    main()
