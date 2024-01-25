# Word Count Project

## Description

This project is a simple word count program that takes a text file as input and outputs the number of words in the file. The program is written in Python and only uses the standard library.

## Design

### Driver

the driver is a http server that listens for requests from workers. It starts with a input file/directory, and the number of mappers and reducers. and generates a queue of tasks to be executed by the workers.

A task contains the information that needs to be passed to the worker to execute the map and reduce function.

The task queue is a global FIFO queue that contains all the tasks and store in memory.

The driver exposes a Restful API `GET:/get_task` that returns a task to the caller.

### Worker

The worker implements the map and reduce function. It starts with the driver host and port, and check whether the driver is available. and request a task from the driver. After the task is finished, it will request another task from the driver. The worker will exit when the driver is unavailable.

#### Mapper function

The mapper function takes a file as input, and split the file into words. Write the word as new line to a new local file named `mr-<task-id>-<word[0]%number_reduce>`.

#### Reducer function

The reducer function takes a list of files as input, and count the number of lines in each file. The sum of the number of lines is the number of words in the input file. write the result to a local file named `out-<reducer_id>`.

### How they interact

after the driver start, it exposes a HTTP endpoint `GET:/get_task` and wait request from the worker.
Every time a worker request a task, the driver will pop a task from the global task queue and return it to the worker.
When the global queue is empty, the driver wait 10s and exit.

The worker will then execute the task and write the result to the local file. After a task is finished, the worker will request another task from the driver. When the server is closed, the worker will exit. If the worker start and find out there is no driver listenning on the port, it will wait for driver start than request a task.

## Usage

Start the driver

```bash
python driver.py  <input_file> <M> <N> 
```

- `input_file`: The path to the input file/directory to be processed. default `inputs/`
- `M`: The number of mappers to use. Defaults to 1.
- `N`: The number of reducers to use. Defaults to 1.

Start the worker

```bash
python worker.py <driver_host> <driver_port> <delay_seconds>
```

- `driver_host`: The hostname of the driver. default local host
- `driver_port`: The port the driver is listening on. default 8080
- `delay_seconds`: the number of seconds to wait after a task is finished. default 0. Could use for testing.

## Issues

These issues are not fixed due to time limit.

- The worker opens the intermediate files in append mode, which means if the worker is killed during the execution or running on same directory with old intermediate file undeleted, the intermediate files will be corrupted. This could be fixed by using a temporary file and rename it after the task is finished.
- The worker does not check whether the task is finished successfully. This could be fixed by adding a status field in the task and check the status after the task is finished.
- The server is statefull, which means if the server is killed during the execution, the task queue will be lost. This could be fixed by using a persistent storage for the task queue.

## Improvements

> some of these improvements requires 3rd party libraries, which is not allowed in this project.

- more and friendly configuration options
- better error handling
- better logging and tracing
- better task scheduling and distribution
- Distributed file system support
