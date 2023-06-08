# Word Count Project

## Description

This project is a simple word count program that takes a text file as input and outputs the number of words in the file. The program is written in Python and only uses the standard library.

## Usage

there are two comonents to this project: the driver and the worker.
the driver is responsible for splitting the input into tasks and assigning them to workers.
the worker is responsible for requesting tasks from the driver, executing the map and reduce task, and write the output to a file.

Start the driver

```bash
python driver.py <port> <input_file> <M> <N> 
```

Start the worker

```bash
python worker.py <driver_host> <driver_port>
```


### Parameters

- `input_file`: The path to the input file/directory to be processed.
- `M`: The number of mappers to use. Defaults to 1.
- `N`: The number of reducers to use. Defaults to 1.
- `output_file`: The path to the output directory. Defaults to `output/`.
