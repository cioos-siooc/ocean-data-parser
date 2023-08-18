The ocean-data-parser provides a `odpy [ODPY OPTIONS] compile [compile OPTIONS]` method to convert compatible files to netcdf.

::: mkdocs-click
    :module: ocean_data_parser.cli
    :command: inspect_variables

!!! Tip "Environment Variables"
    All the inputs available within the `odpy` command can be defined through environment variables:  `ODPY_*`, `ODPY_CONVERT_*` and `ODPY_COMPILE_*` respectively. 

    Example: 
    - `ODPY_LOG_LEVEL=WARNING` will force `odpy` to log only the warning events.
    - `ODPY_CONVERT_OUTPUT_PATH=output` will force `odpy convert` to output to the the local directory `./output/`

!!! Warning
    An argument take priority over an environment variable.


