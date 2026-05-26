
![Logo](docs/images/logo_EN_FR-1024x208.png#gh-light-mode-only)
![Logo](docs/images/cioos-national_EN_FR_W-01.png#gh-dark-mode-only)

# ocean-data-parser

<!-- You can get project-relevant badges from: [shields.io](https://shields.io/) -->

[![Build documentation](https://github.com/cioos-siooc/ocean-data-parser/actions/workflows/deploy-docs.yaml/badge.svg)](https://github.com/cioos-siooc/ocean-data-parser/actions/workflows/deploy-docs.yaml)

`ocean-data-parser` - a Python package for parsing oceanographic proprietary data formats to xarray Datasets[^1].

<div align = center>
<a href='https://cioos-siooc.github.io/ocean-data-parser/'><kbd> <br> See Full Documentation Here <br> </kbd></a>
</div>

## Installation

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

Activate the new environment:

```console
source .venv/bin/activate
```

Test the install:

```console
odpy --version
```

### How to

#### odpy cli

Once installed, the package is usable via the command line interface:

```console
odpy --help
```

To batch convert a series of files to NetCDF:

```
odpy convert -i '**/*.cnv' -p 'seabird.cnv'
```

#### Format auto-detection

Load a compatible file with the automated parser detection method:

```python
import ocean_data_parser.read

# Load a file to an xarray object
ds = ocean_data_parser.read.file('Path to file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```

!!!warning
    The parser detection method relies on the file extension and the first few lines present within the given file.

Or specify the specific parser to use for this file format:

``` python
from ocean_data_parser.parsers import seabird

# Load a seabird cnv file as an xarray dataset
ds = seabird.cnv('Path to seabird cnv file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```

The `ocean-data-parser` can then be used within either a Python package, script or jupyter notebook. See the [documentation Notebook section](https://cioos-siooc.github.io/ocean-data-parser) for examples of how to use the package within a jupyter notebook.

## Contributions

All contributions are welcome!

Please create a new [discussion](https://github.com/cioos-siooc/ocean-data-parser/discussions) or [issue](https://github.com/cioos-siooc/ocean-data-parser/issues) within the GitHub repository for any questions, ideas and suggestions.

[^1]: [Xarray package documentation](https://docs.xarray.dev/en/stable/index.html)
