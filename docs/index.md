---
hide:
  - navigation
  - title
title: Ocean Data Parser Documentation
template: home.html
---

#

## :octicons-download-24: Installation

Install the package in a local environment via the following command:

```console
pip install git+https://github.com/cioos-siooc/ocean-data-parser.git
```

## How to 

### :octicons-command-palette-24: via Command Line Interface `odpy`

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

###  :material-file-find: via `ocean_data_parser.read.file`
Load a compatible file with the global read.file method

```py title="from ocean_data_parser import read"
from ocean_data_parser import read

# Load a file to an xarray object
ds = read.file('Path to file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```

###  :material-sitemap-outline: via `from ocean_data_parser.parsers import ...`
Or specify the specific parser to use for this file format:

```py title="from ocean_data_parser.parsers import ..."
from ocean_data_parser.parsers import seabird

# Load a seabird cnv file as an xarray dataset
ds = seabird.cnv('Path to seabird cnv file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```
