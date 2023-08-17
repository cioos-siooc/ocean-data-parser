### Conversion

??? question "Convert files to netcdf"

    You can use the odpy command within a terminal to access the different functions of the `ocean-data-parser`.

    Convert all cnv files in subdiretories by using the seabird.cnv parser and save to output directory:
    ```console
    odpy convert --input-path input/**/*.cnv --parser=seabird.cnv --output_path=output
    ``` 

    See [commmand line section](../user_guide/cli.md) for more detail or use the command: `odpy convert --help`

??? question "Avoid reconverting over again the same files"

    The `ocean-data-parser` provides a file retristry which can be used to:
    - track which files were converted and outputted where
    - error associated with each files
    - file modified time 
    - file hash

    If activated, a registry file (*.csv/*.parquet) will be saved and any time a conversion is rerun. odpy will first compare the available files to the already parsed files available within the registry and will only convert the ones which have changes. Those changes are primarily based on the file modified time, each modified file is then rehashed and if that hash is different the file will be reconverted and the output overwritten (default)

### Parser handling

??? question "Load any compatible files in my own project"


    To load a compatible file you can use the automated parser detection method:

    ```python
    from ocean_data_parser import read

    # Load a file to an xarray object
    ds = read.file('Path to file')

    # Save to netcdf
    ds.to_netcdf('save-path.nc')
    ```

    :warning: The parser detection method relies on the file extension and the first few lines present within the given file. It is preferable to define a specific parser when a tool is used in production.



??? question "Load a file with a specific parser in my own project"
    You can import a specific parser via the `ocean_data_parser.parser`
    ``` python
    from ocean_data_parser.parsers import seabird

    # Load a seabird cnv file as an xarray dataset
    ds = seabird.cnv('Path to seabird cnv file')

    # Save to netcdf
    ds.to_netcdf('save-path.nc')
    ```
    The `ocean-data-parser` can then be used within either a python package, script or jupyter notebook. See [documentation Notebook section](https://cioos-siooc.github.io/ocean-data-parser) for examples on how to use the package within a jupyter notebook.