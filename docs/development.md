# Development

This page contains all the information necessary to setup a local development environment.

## :octicons-person-add-24: Contribution

All contributions are welcome!

Please create a new [discussion](https://github.com/cioos-siooc/ocean-data-parser/discussions)
or [issue](https://github.com/cioos-siooc/ocean-data-parser/issues) within the
github repository for any questions, ideas and suggestions.

## :octicons-download-24: Installation

Clone the project locally

```console
  git clone git+https://github.com/cioos-siooc/ocean-data-parser.git
```

Install the package manger uv python environment, see [uv docs](https://docs.astral.sh/uv/getting-started/installation/) for details.

Go to the project directory and install all the development dependancies in a uv environment.

```console
  cd ocean-data-parser
  uv venv
  uv pip install ".[dev]"
```

## :material-test-tube: Testing

The package use pytest to run a series of tests in order to help the development of the different aspect of the package. Within a developping environment, to run the different tests, run the pytest commmand through your terminal within the base directory of the repository. Pytest can also be integrated with different IDE and is run on any pushes and PR to the `main` and `development` branches.

## :material-checkbox-multiple-marked-circle-outline: Parsers Tests

The package contains a number of different parsers compatible with different standard file formats. Each parser is tested on a series of test files made available within the [test file directory](https://github.com/cioos-siooc/ocean-data-parser/blob/main/tests/parsers_test_files) The different tests executed on each individual parsers can be subdivided in 3 main categories:

1. Parse test file to an xarray dataset
2. Parse test file to an xarray dataset and save to a NetCDF4 file.
3. Parse test file to an xarray dataset and compare to a reference file ('\*\_reference.nc) if made available. Any differences are flagged
4. _(in development)_ Assess parsed xarray object compliance with the different convention by using the ioos-compliance checker, resulting objects should be to a minimum compliante to ACDD 1.3 and CF 1.6. Other conventions can be added by adding them to the xarray object global attribute `Convention`.

## :octicons-book-24: Documentation Build

To run a local instance of the documentation webpage. Install the dependancies:

```console
uv pip install ".[docs]"
```

And run the command:

```console
uv run mkdocs serve
```

Any documentation changes to the main and development branches will automatically update respectively the main and dev document pages.
