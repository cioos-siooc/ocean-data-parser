
Clone the project locally

```console
  git clone git+https://github.com/cioos-siooc/ocean-data-parser.git
```

We recommend using [poetry](https://python-poetry.org/) to manage the environment and [pyenv](https://github.com/pyenv/pyenv) to manage the python version:

```console
pyenv install 3.11.2
pyenv shell 3.11.2
```

Install poetry python environment with:

```console
pip install poetry
```

Go to the project directory and install dependancies in a poetry environment.

```console
  cd ocean-data-parser
  poetry install --with dev
```

## Testing

The package use pytest to run a series of tests in order to help the development of the different aspect of the package. Within a developping environment, to run the different tests, run the pytest commmand through your terminal within the base directory of the repository. Pytest can also be integrated with different IDE and is run on any pushes and PR to the `main` and `development` branches.

## Parsers Tests

The package contains a number of different parsers compatible with different standard file formats. Each parser is tested on a series of test files made available within the [test file directory](https://github.com/cioos-siooc/ocean-data-parser/blob/main/tests/parsers_test_files) The different tests executed on each individual parsers can be subdivided in 3 main categories:

1. Parse test file to an xarray dataset
2. Parse test file to an xarray dataset and save to a NetCDF4 file.
3. Parse test file to an xarray dataset and compare to a reference file ('*_reference.nc) if made available. Any differences are flagged
4. *(in development)* Assess parsed xarray object compliance with the different convention by using the ioos-compliance checker, resulting objects should be to a minimum compliante to ACDD 1.3 and CF 1.6. Other conventions can be added by adding them to the xarray object global attribute `Convention`.

## Documentation

To run a local instance of the documentation webpage. Install the dependancies:

```console
poetry install --group docs
```

And run the command:

```console
poetry run mkdocs serve
```

Any documentation changes to the main and development branches will automatically update respectively the main and dev document pages.
