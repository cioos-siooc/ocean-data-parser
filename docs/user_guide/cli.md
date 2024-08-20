---
title: odpy CLI
---
# Command Line Interface

::: mkdocs-click
    :module: ocean_data_parser.cli
    :command: main
    :depth: 0

!!! Tip "Environment Variables"
    All the inputs available within the `odpy` command can be defined through environment variables:  `ODPY_*`, `ODPY_CONVERT_*` and `ODPY_COMPILE_*` respectively.

    Example: 
    - `ODPY_LOG_LEVEL=WARNING` will force `odpy` to log only the warning events.
    - `ODPY_CONVERT_OUTPUT_PATH=output` will force `odpy convert` to output to the the local directory `./output/`


## Configuration

`odpy convert` can handle a configuration file which also integrates a number of other features. To generate a new configuration, you can use the `--new-config` flag:

```shell
odpy convert --new-config new_config_file_path.yaml
```

Or copy the following yaml:

```yaml
--8<-- "ocean_data_parser/batch/default-batch-config.yaml"
```
