# [aiutil](https://github.com/dclong/aiutil): Data Science Utils

This is a Python pacakage that contains misc utils for Data Science.

1. Misc enhancement of Python's built-in functionalities.
    - string
    - collections
    - pandas DataFrame
    - datetime
2. Misc other tools
    - `aiutil.git`: check and report modified but unpushed repository under a directory recursively
    - `aiutil.filesystem`: misc tools for querying and manipulating filesystems; convenient tools for manipulating text files.
    - `aiutil.url`: URL formatting for HTML, Excel, etc.
    - `aiutil.sql`: SQL formatting
    - `aiutil.cv`: some more tools (in addition to OpenCV) for image processing
    - `aiutil.shell`: parse command-line output to a pandas DataFrame
    - `aiutil.shebang`: auto correct SheBang of scripts
    - `aiutil.poetry`: tools for making it even easier to manage Python project using Poetry
    - `aiutil.pdf`: easy and flexible extracting of PDF pages
    - `aiutil.memory`: query and consume memory to a specified range
    - `aiutil.jupyter`: Jupyter/Lab notebook related tools (cell code formating, converting, etc.)
    - `aiutil.dockerhub`: managing Docker images on DockerHub in batch mode using Python
    - `aiutil.hadoop`: 
        - A Spark application log analyzing tool for identify root causes of failed Spark applications.
        - Pythonic wrappers to the `hdfs` command.
        - A auto authentication tool for Kerberos.
        - An improved version of `spark_submit`.
        - Other misc PySpark functions. 
    
## Supported Operating Systems and Python Versions

| OS      | Python 3.7 | Python 3.8 | Python 3.9 | Python 3.10 |
|---------|------------|------------|------------|-------------|
| Linux   | Y          | Y          | Y          | Y           |
| macOS   | Y          | Y          | Y          | Y           |
| Windows | Y          | Y          | Y          | Y           |

## Installation

```bash
pip3 install --user -U aiutil
```
Use the following commands if you want to install all components of aiutil. 
Available additional components are `cv`, `docker`, `pdf`, `jupyter`, `admin` and `all`.
```bash
pip3 install --user -U aiutil[all]
```
