<!-- NOTE: All sections are placeholders. Use the relevant ones-->

![Logo](docs/images/logo_EN_FR-1024x208.png#gh-light-mode-only)
![Logo](docs/images/cioos-national_EN_FR_W-01.png#gh-dark-mode-only)

<!-- Make a favicon/logo using something like:

* https://favicon.io/
* https://www.shopify.com/tools/logo-maker/open-source-software
* https://primitive.lol/ -->

# ocean-data-parser

<!-- You can get project relevant badges from: [shields.io](https://shields.io/) -->

[![Build documentation](https://github.com/cioos-siooc/ocean-data-parser/actions/workflows/deploy-docs.yaml/badge.svg)](https://github.com/cioos-siooc/ocean-data-parser/actions/workflows/deploy-docs.yaml)


`ocean-data-parser` - a python package for parsing oceanographic proprietary data formats to [xarray Dataset](https://docs.xarray.dev/en/stable/).

[A more detailed documentation is available here](https://cioos-siooc.github.io/ocean-data-parser/).

## Installation
Install the package with the following command, ideally within a virtual environment:

```console
pip install git+https://github.com/cioos-siooc/ocean-data-parser.git
```

### Examples
#### odpy cli
Once installed, the package is usable via the command line interface:
```console
odpy --help
```

To batch convert a series of files to netcdf:
```
odpy convert -i '**/*.cnv' -p 'seabird.cnv'
```
#### format auto-detection
Load a compatible file with the automated parser detection method:

```python
import ocean_data_parser.parsers

# Load a file to an xarray object
ds = ocean_data_parser.parsers.file('Path to file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```
> :warning: The parser detection method relies on the file extension and the first few lines present within the given file.

Or specify the specific parser to use for this file format:
``` python
from ocean_data_parser.parsers import seabird

# Load a seabird cnv file as an xarray dataset
ds = seabird.cnv('Path to seabird cnv file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```
The `ocean-data-parser` can then be used within either a python package, script or jupyter notebook. See [documentation Notebook section](https://cioos-siooc.github.io/ocean-data-parser) for examples on how to use the package within a jupyter notebook.

## Contributions

All contributions are welcome! 

Please create a new [discussion](https://github.com/cioos-siooc/ocean-data-parser/discussions) or [issue](https://github.com/cioos-siooc/ocean-data-parser/issues) within the github repository for any questions, ideas and suggestions. 
