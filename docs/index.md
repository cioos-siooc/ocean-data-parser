---
hide:
  - navigation
  - title
title: Ocean Data Parser Documentation
template: home.html
---

#

## :octicons-download-24: Installation

First, install the [uv package manager](https://github.com/astral-sh/uv)
```console
pip install uv
```

Next, clone the repository to your local machine, enter the project directory and use `uv sync` to setup the package.

```console
git clone https://github.com/cioos-siooc/ocean-data-parser
cd ocean-data-parser
uv sync --python 3.9
```

This process will create a Python 3.9 virtual environment in the a `.venv` directory and populate it with the packages described in the `pyproject.toml` and `uv.lock` files.

Activate the new environment
```console
source .venv/bin/activate
```

Test the install
```console
odpy --version
```

## How to 

### [:octicons-command-palette-24: via Command Line Interface `odpy`](user_guide/cli.md)

Once installed, the package is usable via the command line interface 
via the `odpy` command. As an example to convert a series of cnv files to netcdf, 
you can use the following command:

```console
odpy convert -i '**/*.cnv'
```

For futher details see [here](user_guide/cli.md) or run the following command:

```console
odpy --help 
```

### [:material-file-find: via `ocean_data_parser.read.file`](user_guide/read.md)

Load a compatible file with the global read.file method

```py title="from ocean_data_parser import read"
from ocean_data_parser import read

# Load a file to an xarray object
ds = read.file('Path to file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```

### [:material-sitemap-outline: via `from ocean_data_parser.parsers import ...`](user_guide/parsers/index.md)

Or specify the specific parser to use for this file format:

```py title="from ocean_data_parser.parsers import ..."
from ocean_data_parser.parsers import seabird

# Load a seabird cnv file as an xarray dataset
ds = seabird.cnv('Path to seabird cnv file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```
