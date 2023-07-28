<!-- NOTE: All sections are placeholders. Use the relevant ones-->

![Logo](docs/images/logo_EN_FR-1024x208.png)

<!-- Make a favicon/logo using something like:

* https://favicon.io/
* https://www.shopify.com/tools/logo-maker/open-source-software
* https://primitive.lol/ -->

# ocean-data-parser

<!-- You can get project relevant badges from: [shields.io](https://shields.io/) -->

[![Update gh-pages Docs](https://github.com/HakaiInstitute/ocean-data-parser/actions/workflows/generate-documentation.yaml/badge.svg)](https://github.com/HakaiInstitute/ocean-data-parser/actions/workflows/generate-documentation.yaml)

## What is it?

The `ocean-data-parser` is a set of tools capable of parsing oceanographic proprietary data formats to a xarray CIOOS Compliant object. This xarray object can then be use in a number of application and/or easily saved to a NetCDF format.

A more detailed documentation is available [here](https://hakaiinstitute.github.io/ocean-data-parser).

---

## Table of Contents

<details>

<summary>Table of Contents</summary>

- [ocean-data-parser](#ocean-data-parser)
  - [What is it?](#what-is-it)
  - [Table of Contents](#table-of-contents)
  - [How to](#how-to)
  - [Development](#development)
    - [Installation](#installation)
    - [Documentation](#documentation)
    - [Testing](#testing)
      - [Parsers Tests](#parsers-tests)
  - [Contributing](#contributing)

</details>

---

## How to

Install the package with the following command, ideally within a virtual environment:

```env
pip install git+https://github.com/HakaiInstitute/ocean-data-parser.git
```

As an example, to load a compatible file you can use the automated parser detection method:

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
The `ocean-data-parser` can then be used within either a python package, script or jupyter notebook. See [documentation Notebook section](https://hakaiinstitute.github.io/ocean-data-parser) for examples on how to use the package within a jupyter notebook.

## Development

### Installation
Clone the project locally

```shell
  git clone git+https://github.com/HakaiInstitute/ocean-data-parser.git
```

Go to the project directory

```shell
  cd ocean-data-parser
```

Install dependencies

```shell
  pip install -e ".[dev]"
```

### Documentation
To run a local instance of the documentation webpage, run the command:

```shell
mike serve
```

Any documentation changes to the main and development branches will automatically update respectively the main and dev document pages.

### Testing
The package use pytest to run a series of tests in order to help the development of the different aspect of the package. Within a developping environment, to run the different tests, run the pytest commmand through your terminal within the base directory of the repository. Pytest can also be integrated with different IDE and is run on any pushes and PR to the `main` and `development` branches.

#### Parsers Tests
The package contains a number of different parsers compatible with different standard file formats. Each parser is tested on a series of test files made available within the [test file directory](tests/parsers_test_files) The different tests executed on each individual parsers can be subdivided in 3 main categories:
1. Parse test file to an xarray dataset
2. Parse test file to an xarray dataset and save to a NetCDF4 file.
3. Parse test file to an xarray dataset and compare to a reference file ('*_reference.nc) if made available. Any differences are flagged
4. *(in development)* Assess parsed xarray object compliance with the different convention by using the ioos-compliance checker, resulting objects should be to a minimum compliante to ACDD 1.3 and CF 1.6. Other conventions can be added by adding them to the xarray object global attribute `Convention`.

## Contributing

All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome.