# [aiutil](https://github.com/legendu-net/aiutil): Data Science Utils

This is a Python pacakage that contains misc utils for Data Science.

1. Misc enhancement of Python's built-in functionalities.
    - string
    - collections
    - pandas DataFrame
    - datetime
2. Misc other tools
    - `aiutil.filesystem`: misc tools for querying and manipulating filesystems; convenient tools for manipulating text files.
    - `aiutil.url`: URL formatting for HTML, Excel, etc.
    - `aiutil.sql`: SQL formatting
    - `aiutil.cv`: some more tools (in addition to OpenCV) for image processing
    - `aiutil.shell`: parse command-line output to a pandas DataFrame
    - `aiutil.shebang`: auto correct SheBang of scripts
    - `aiutil.poetry`: tools for making it even easier to manage Python project using Poetry
    - `aiutil.pdf`: easy and flexible extracting of PDF pages
    - `aiutil.memory`: query and consume memory to a specified range
    - `aiutil.notebook`: Jupyter/Lab notebook related tools
    - `aiutil.dockerhub`: managing Docker images on DockerHub in batch mode using Python
    - `aiutil.hadoop`: 
        - A Spark application log analyzing tool for identify root causes of failed Spark applications.
        - Pythonic wrappers to the `hdfs` command.
        - A auto authentication tool for Kerberos.
        - An improved version of `spark_submit`.
        - Other misc PySpark functions. 
    
## Supported Operating Systems and Python Versions

Python 3.10.x on Linux and macOS.
It might work on Windows but is not tested on Windows.

## Installation

```bash
pip3 install --user -U aiutil
```
Use the following commands if you want to install all components of aiutil. 
Available additional components are `cv`, `docker`, `pdf`, `jupyter`, `admin` and `all`.
```bash
pip3 install --user -U aiutil[all]
```
