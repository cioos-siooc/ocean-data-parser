# User Guide

## How to
The ocean-data-parser can be used different ways:

- [ ] [Command Line interface](cli.md)

- [ ] Python Import

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

    ``` shell
    # if branch=test-branch
    pip install git+git+https://github.com/cioos-siooc/ocean-data-parser.git@test-branch
    ```

=== "Specific tag"

    ``` shell
    # if `tag=v1.2`
    pip install git+git+https://github.com/cioos-siooc/ocean-data-parser.git@v1.2
    ```

=== "Specific hash"

    ``` shell
    # if hash=2927346f4c513a217ac8ad076e494dd1adbf70e1
    pip install git+git+https://github.com/cioos-siooc/ocean-data-parser.git@2927346f4c513a217ac8ad076e494dd1adbf70e1
    ```

## Integration within a project

We recommmand fixing the `ocean_data_parser` used within your project since this
package api is still subject to change over the future versions.

You can achieve that either:

- with `pip` by generating a `requirement.txt`
- (recommanded) with [`poetry`](https://python-poetry.org/) by creating a `pyproject.toml` and `poetry.lock` file.

=== "poetry"

    ``` shell
    mkdir my_project
    cd my_project

    # initialize poetry package 
    poetry init
    # add package via instruction 
    # or with the following command once the pyproject.toml is generated
    poetry add git+https://github.com/cioos-siooc/ocean-data-parser.git

    # install packages in local environment.
    poetry install

    # A new file pyproject.toml should be generated with a poetry.lock file 
    # which list the specific version of each package installed. 
    ```

=== "pip"

    ``` shell
    mkdir my_project
    cd my_project

    pip install git+https://github.com/cioos-siooc/ocean-data-parser.git
    pip freeze > requirements.txt

    # use next time
    pip install -r requirement.txt
    ```

!!! info "Poetry package-mode"

    If you only use poetry for dependancy management and tracking versions used
    in your project. Add the following snippet in our `pyproject.toml`:
    
    ``` toml
    [tool.poetry]
    package-mode = false
    ```
