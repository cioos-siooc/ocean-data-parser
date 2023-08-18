The ocean-data-parser provides a `odpy [ODPY OPTIONS] convert [convert OPTIONS]` method to convert compatible files to netcdf.

::: mkdocs-click
    :module: ocean_data_parser.cli
    :command: convert
    :depth: 0

!!! Tip "Environment Variables"
    All the inputs available within the `odpy` command can be defined through environment variables:  `ODPY_*`, `ODPY_CONVERT_*` and `ODPY_COMPILE_*` respectively. 

    Example: 
    - `ODPY_LOG_LEVEL=WARNING` will force `odpy` to log only the warning events.
    - `ODPY_CONVERT_OUTPUT_PATH=output` will force `odpy convert` to output to the the local directory `./output/`

!!! Warning
    An argument take priority over an environment variable.

# configuration file
`odpy convert` can be used via a configuration file like the following:

``` {.yaml title="odpy-convert-config.yaml" .annotate}
--8<-- "ocean_data_parser/batch/default-batch-config.yaml"
```

!!! info
    A new configuration file can be generated via `odpy convert`:

    ```console
    odpy convert --new-config path/to/config.yaml
    ```




