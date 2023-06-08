#! /usr/bin/env python3

import collections
import http.client
import json
import os
import re
import socket
import sys
import time


def map_task(
    input_file: str | list[str],
    map_task_id: int,
    num_reduce_tasks: int,
    output_dir: str = "intermediate",
):
    # Open the input file
    if type(input_file) == str:
        input_file = [input_file]
    for file in input_file:
        with open(file, "r") as f:
            data = f.read()
            # Split the text into words
            words = re.findall(r"\w+", data)
            print(f"map task processing file {file} with {len(words)} words")

            # Create a bucket for each reduce task
            buckets = [[] for _ in range(num_reduce_tasks)]

            # Assign each word to a bucket
            for word in words:
                bucket_id = ord(word[0].lower()) % num_reduce_tasks
                buckets[bucket_id].append(word)

            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            # Write each bucket to an intermediate file
            for bucket_id, bucket in enumerate(buckets):
                with open(f"{output_dir}/mr-{map_task_id}-{bucket_id}", "w") as f:
                    for word in bucket:
                        f.write(word + "\n")


def reduce_task(
    bucket_id: int,
    map_task_ids: list[int],
    input_dir: str = "intermediate",
    output_dir: str = "out",
):
    # Initialize a counter for word frequencies
    word_counts = collections.Counter()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Process each intermediate file for this bucket
    for map_task_id in map_task_ids:
        with open(f"{input_dir}/mr-{map_task_id}-{bucket_id}", "r") as f:
            words = f.read().splitlines()
            print(
                f"reduce task processing file mr-{map_task_id}-{bucket_id} with {len(words)} words"
            )
            word_counts.update(words)

    # Write the word counts to the output file

    with open(f"{output_dir}/out-{bucket_id}", "w") as f:
        for word, count in word_counts.items():
            f.write(f"{word} {count}\n")

def parse_args()-> tuple[str, int]:
    if len(sys.argv) == 1:
        print("driver host and port not specified, use default localhost:8080")
        return "localhost", 8080
    if len(sys.argv) != 3:
        print("Usage: python3 worker.py <worker_id> <port>")
        sys.exit(0)
    return sys.argv[1], int(sys.argv[2])

def run(host: str, port: int):
    while True:
        try:
            conn = http.client.HTTPConnection(host=host, port=port)
            conn.request("GET", "/get_task")
            response = conn.getresponse()
            data = response.read()
            resp = json.loads(data)
            if resp["task_type"] == "map":
                map_task(resp["input_files"], resp["task_id"], resp["num_reduce_tasks"])
                pass
            elif resp["task_type"] == "reduce":
                reduce_task(resp["bucket_id"], resp["map_task_ids"])
                pass
            time.sleep(10)
        except Exception as e:
            print("server closed")
            break


def wait_driver_start():
    while True:
        try:
            print("check driver start")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                ss.connect(("localhost", 8080))
                ss.close()
                print("driver started")
                break
        except Exception as e:
            print("driver not started, wait 1 second")
            time.sleep(1)
            pass


def wait_driver_exit():
    # wait for dirver exit
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ss:
                ss.connect(("localhost", 8080))
                ss.close()
                print("dirver not exit, wait 1 second")
                time.sleep(1)
        except Exception as e:
            print("driver closed, worker exit")
            break


if __name__ == "__main__":
    driver_addr, driver_port = parse_args()
    wait_driver_start()
    run(driver_addr, driver_port)
    wait_driver_exit()
