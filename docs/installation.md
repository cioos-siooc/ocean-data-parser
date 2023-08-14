## PIP 

Install the package with the following command, ideally within a virtual environment:

```console
pip install git+https://github.com/cioos-siooc/ocean-data-parser.git
```

Once installed, you can test the package through the command line with:

```console
odpy --help
```
!!! Info
    For more details on how to use the command line interface see [commmand line section](cli.md).

## Development

### Installation
Clone the project locally

```shell
  git clone git+https://github.com/cioos-siooc/ocean-data-parser.git
```

Go to the project directory

```shell
  cd ocean-data-parser
```

Install dependencies

```shell
  pip install -e ".[dev]"
```

### Testing
The package use pytest to run a series of tests in order to help the development of the different aspect of the package. Within a developping environment, to run the different tests, run the pytest commmand through your terminal within the base directory of the repository. Pytest can also be integrated with different IDE and is run on any pushes and PR to the `main` and `development` branches.

### Parsers Tests
The package contains a number of different parsers compatible with different standard file formats. Each parser is tested on a series of test files made available within the [test file directory](tests/parsers_test_files) The different tests executed on each individual parsers can be subdivided in 3 main categories:
1. Parse test file to an xarray dataset
2. Parse test file to an xarray dataset and save to a NetCDF4 file.
3. Parse test file to an xarray dataset and compare to a reference file ('*_reference.nc) if made available. Any differences are flagged
4. *(in development)* Assess parsed xarray object compliance with the different convention by using the ioos-compliance checker, resulting objects should be to a minimum compliante to ACDD 1.3 and CF 1.6. Other conventions can be added by adding them to the xarray object global attribute `Convention`.


### Documentation
To run a local instance of the documentation webpage. Install the dependancies:
```console
pip install -r docs-requirements.txt
```

And run the command:

```shell
mike serve
```

Any documentation changes to the main and development branches will automatically update respectively the main and dev document pages.

