---
hide:
  - navigation
  - title
title: Ocean Data Parser Documentation
template: home.html
---
#
## Installation
Install the package in a local environment via the following command:

```console
pip install git+https://github.com/cioos-siooc/ocean-data-parser.git
```

## How to
### via Command Line Interface `odpy`
Once installed, the package is usable via the command line interface:
```console
odpy --help
```

To batch convert a series of files to NetCDF:
```
odpy convert -i '**/*.cnv'
```

For futher details see [here](user_guide/cli.md) or run the following command:
```console
odpy --help 
```
### Within a python Script

#### Using read.file
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

#### Using importing the parser itself
Or specify the specific parser to use for this file format:
``` python
from ocean_data_parser.parsers import seabird

# Load a seabird cnv file as an xarray dataset
ds = seabird.cnv('Path to seabird cnv file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```

