#! /usr/bin/env python3

import http.server
import json
import os
import socketserver
import sys
import time
from queue import Queue


def split_list(input_list, num_slices):
    slice_length = len(input_list) // num_slices
    remainder = len(input_list) % num_slices
    slices = []
    start = 0
    for i in range(num_slices):
        if i < remainder:
            end = start + slice_length + 1
        else:
            end = start + slice_length

        slices.append(input_list[start:end])
        start = end
    return slices


def assign_tasks(
    input_files: list[str], num_map_tasks: int, num_reduce_tasks: int
) -> Queue:
    # Create a list of tasks
    tasks = Queue()
    # Assign map tasks
    files_per_map_task = split_list(input_files, num_map_tasks)
    for i in range(num_map_tasks):
        tasks.put(
            {
                "task_type": "map",
                "input_files": files_per_map_task[i],
                "task_id": i,
                "num_reduce_tasks": num_reduce_tasks,
            }
        )

    # Assign reduce tasks
    for i in range(num_reduce_tasks):
        map_task_ids = [j for j in range(num_map_tasks)]
        tasks.put({"task_type": "reduce", "bucket_id": i, "map_task_ids": map_task_ids})

    return tasks


tasks: Queue = Queue()


def parse_args() -> tuple[list[str], int, int]:
    if len(sys.argv) == 1:
        return ([f for f in os.listdir("inputs") if not f.startswith(".")], 1, 1)
    if len(sys.argv) != 4:
        print(
            "Usage: python3 driver.py <input_file> <num_map_tasks> <num_reduce_tasks>"
        )
        sys.exit(0)
    input_file = sys.argv[1]
    num_map_tasks = int(sys.argv[2])
    num_reduce_tasks = int(sys.argv[3])
    input_files = []
    if os.path.isdir(input_file):
        for file in os.listdir(input_file):
            if not file.startswith("."):
                input_files.append(os.path.join(input_file, file))
    elif os.path.isfile(input_file):
        input_files.append(input_file)
    else:
        print("input file is not valid")
        sys.exit(0)
    return (input_files, num_map_tasks, num_reduce_tasks)


class MyHTTPHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/get_task":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            if not tasks.empty():
                task = tasks.get()
                response = json.dumps(task).encode()
                self.wfile.write(response)
                print(f"send task :{task}, rest of the tasks: {tasks.qsize()}")
            if tasks.empty():
                print("all tasks completed, wait 10s then server shutting down")
                time.sleep(10)
                sys.exit(0)
            return
        else:
            self.send_response(404)
            self.end_headers()
            return


def serve():
    if tasks.empty():
        print("no task to serve, server shutting down")
        sys.exit(0)
    with socketserver.TCPServer(("", 8080), MyHTTPHandler) as httpd:
        print("serving at port", 8080)
        httpd.serve_forever()


if __name__ == "__main__":
    input_files, num_map_tasks, num_reduce_tasks = parse_args()
    print(
        f"dirver start with input files: {input_files}, num_map_tasks: {num_map_tasks}, num_reduce_tasks: {num_reduce_tasks}"
    )
    tasks = assign_tasks(input_files, num_map_tasks, num_reduce_tasks)
    print(f"driver start to serve with {tasks.qsize()} number of task")
    serve()
