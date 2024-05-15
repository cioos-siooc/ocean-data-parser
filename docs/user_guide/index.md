# User Guide

## How to
The ocean-data-parser can be used different ways:

1. [Command Line interface](cli.md)

2. Python Import

    - [`from ocean_data_parser import read.file`](read.md)
    - [`from ocean_data_parser.parsers import ...`](parsers/index.md)

## Installation

Install the github development version via pip. You can also install a specific branch, tag or hash by following the present examples:


=== "Default"

    ```console
    # install the latest development branch version
    pip install git+git+https://github.com/cioos-siooc/ocean-data-parser.git
    ```

=== "Specific branch"

    ```shell
    # if branch=test-branch
    pip install git+git+https://github.com/cioos-siooc/ocean-data-parser.git@test-branch
    ```

=== "Specific tag"


    ```shell
    # if `tag=v1.2`
    pip install git+git+https://github.com/cioos-siooc/ocean-data-parser.git@v1.2
    ```

=== "Specific hash"

    ```shell
    # if hash=2927346f4c513a217ac8ad076e494dd1adbf70e1
    pip install git+git+https://github.com/cioos-siooc/ocean-data-parser.git@2927346f4c513a217ac8ad076e494dd1adbf70e1
    ```

## Integration within a project

We recommmand fixing the `ocean_data_parser` used within your project since this
package api is still subject to change over the future versions.

You can achieve that either by generating a `requirement.txt` file with `pip`,
or maintaining a `pyproject.toml` for your specific project with `poetry` (recommended).

=== "poetry"
    
    ```
    mkdir my_project
    cd my_project

    poetry init
    poetry add git+git+https://github.com/cioos-siooc/ocean-data-parser.git
    poetry install

    # A new file pyproject.toml should be generated with a poetry.lock file 
    # which list the specific version of each package installed. 
    ```

=== "pip"
    ```
    mkdir my_project
    cd my_project

    pip install git+git+https://github.com/cioos-siooc/ocean-data-parser.git
    pip freeze > requirements.txt
