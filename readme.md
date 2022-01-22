# [dsutil](https://github.com/dclong/dsutil): Data Science Utils

This is a Python pacakage that contains misc utils for Data Science.

1. Misc enhancement of Python's built-in functionalities.
    - string
    - collections
    - pandas DataFrame
    - datetime
2. Misc other tools
    - git
      ```
      dsutil.git.check
      ```
    - docker
    - text manipulation
    - html
    - misc
    
## Supported Python Version

Currently, Python 3.7 and 3.8 are supported.

## Installation

You can download a copy of the latest release and install it using pip.
```bash
pip3 install --user -U https://github.com/dclong/dsutil/releases/download/v0.69.4/dsutil-0.69.4-py3-none-any.whl
```
Or you can use the following command to install the latest master branch
if you have pip 20.0+.
```bash
pip3 install --user -U git+https://github.com/dclong/dsutil@main
```
Use one of the following commands if you want to install all components of dsutil. 
Available additional components are `cv`, `docker`, `pdf`, `jupyter`, `admin` and `all`.
```bash
pip3 install "dsutil[cv] @ https://github.com/dclong/dsutil/releases/download/v0.69.4/dsutil-0.69.4-py3-none-any.whl"
# or
pip3 install --user -U "dsutil[all] @ git+https://github.com/dclong/dsutil@main"
```
