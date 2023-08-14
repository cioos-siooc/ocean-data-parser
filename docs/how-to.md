## Command line

You can use the odpy command within a terminal to access the different functions of the `ocean-data-parser`.

For more info use the command:
```
odpy --help
```

!!! Info
    For more details on how to use the command line interface see [commmand line section](cli.md).


## Import a specific parser

As an example, to load a compatible file you can use the automated parser detection method:

```python
from ocean_data_parser import read

# Load a file to an xarray object
ds = read.file('Path to file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```

!!! warning
    The parser detection method relies on the file extension and the first few lines present within the given file.

Or specify the specific parser to use for this file format:
``` python
from ocean_data_parser.parsers import seabird

# Load a seabird cnv file as an xarray dataset
ds = seabird.cnv('Path to seabird cnv file')

# Save to netcdf
ds.to_netcdf('save-path.nc')
```
The `ocean-data-parser` can then be used within either a python package, script or jupyter notebook. See [documentation Notebook section](https://cioos-siooc.github.io/ocean-data-parser) for examples on how to use the package within a jupyter notebook.