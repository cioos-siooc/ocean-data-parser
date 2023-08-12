from pathlib import Path
from glob import glob

import click
import pandas as pd
import xarray as xr
from tqdm import tqdm
from loguru import logger



def _get_erddap_dtype(dtype):
    if "float" in str(dtype):
        return "float"
    elif "int" in str(dtype):
        return "int"
    elif dtype in (str, object):
        return "String"
    elif "datetime" in str(dtype):
        return "float"
    else:
        logger.error("Unknown erddap dtype=%s mapping", dtype)
        return dtype


def _get_erddap_xml_variable(variable, destination, type, attrs):
    attr_section = "\n\t\t".join(
        [f'<att name="{key}">{value}</att>' for key, value in attrs.items()]
    )
    return f"""    <dataVariable>
        <sourceName>{variable}</sourceName>
        <destinationName>{destination}</destinationName>
        <dataType>{type}</dataType>
        <addAttributes>\n\t\t{attr_section}
        </addAttributes>
    </dataVariable>"""


@logger.catch(default={})
def _get_netcdf_variables(file) -> list:
    def _get_var_attrs(var, **kwargs):
        return dict(
            file=file, variable=var, dtype=ds[var].dtype, **ds[var].attrs, **kwargs
        )
    logger.contextualize(source_file=file)
    ds = xr.open_dataset(file)
    variables = [_get_var_attrs(var, coords=True) for var in ds.coords]
    variables += [_get_var_attrs(var) for var in ds]
    return variables


def variables(
    input: str = "**/*.nc",
    exclude: str = None,
    groupby: str = "variable,units,standard_name,sdn_parameter_urn,sdn_parameter_name",
    output_table: str = None,
    output_erddap_xml: str = None,
):
    """Compile NetCDF files variables and variables attributes.

    Args:
        input_dir (str, optional): Top directory from where to start
            looking for netCDF files. Defaults to '.'.
        file_regex (str, optional): Glob expression for search NetCDF
            files under subdirectories. Defaults to "**/*.nc".
        groupby (list, optional): List of attributes to regroup the
            different sets of attributes. Defaults to
            ["variable","units","standard_name",
            "sdn_parameter_urn","sdn_parameter_name"]
        output_table (str, optional): Path to file where to output
            table csv or markdown. Defaults console.
        output_erddap_xml (str, optional): Path to where to ouput
            erddap dataset xml. Defaults to console.
    """
    
    # Get file list
    logger.debug("Retrieve files to compile")
    files = glob(input, recursive=True)

    if exclude:
        logger.debug("Retrieve files to ignore")
        excluded_files = glob(exclude, recursive=True)
        files = [file for file in files if file not in excluded_files]
    
    if not files:
        logger.info("No netcdf files available")
        return

    # Compile variable list
    variables = []
    for file in tqdm(files, unit="file", desc="Compile NetCDF variables"):
        variables += _get_netcdf_variables(file)

    # Generate DataFrame
    df_vars = pd.DataFrame(variables)
    logger.debug("columns available: %s", df_vars.columns)
    df_vars_grouped = (
        df_vars.groupby(
            groupby.split(','),
            dropna=False,
        ).agg({"file": "count"})
    ).reset_index()
    df_vars_unique = df_vars.drop_duplicates(subset=["variable", "units"], keep="first")

    # Generate output
    logger.info("Unique Variable list")
    if output_table is None:
        print(df_vars_grouped.to_markdown())
    elif output_table.endswith("md"):
        df_vars_grouped.to_markdown(output_table)
    elif output_table.endswith("csv"):
        df_vars_grouped.to_csv(output_table)

    # Gernerate
    if output_erddap_xml:
        erddap_xml = []
        for id, row in df_vars_unique.iterrows():
            attrs = row.dropna().to_dict()
            file = attrs.pop("file")
            var = attrs.pop("variable")
            dtype = _get_erddap_dtype(attrs.pop("dtype", "String"))
            erddap_xml += [_get_erddap_xml_variable(var, var, dtype, attrs)]
        erddap_xml = "\n".join(erddap_xml)
        if output_erddap_xml in (True, "true", "True", "1"):
            print(erddap_xml)
        else:
            with open(output_erddap_xml, "w") as file_handle:
                file_handle.write(erddap_xml)


@click.command()
@click.option(
    "--input",
    "-i",
    default="**/*.nc",
    show_default=True,
    type=str,
    help="Glob expression of input files",
)
@click.option(
    "--exclude", type=str, help="Glob expression of files to exclude"
)
@click.option(
    "--groupby",
    default="variable,units,standard_name,sdn_parameter_urn,sdn_parameter_name",
    type=str,
    help="Comma separated list of attributes to regroup variables by.",
    show_default=True
)
@click.option(
    "--output_table",
    "-t",
    default=None,
    is_flag=False,
    flag_value='odpy-compile-variables.md',
    help="Result table output: \n - None = console(default)\n - path to csv or markdown(*.md) file.",
)
@click.option(
    "--output_erddap_xml",
    "--xml",
    default=None,
    is_flag=False,
    flag_value='odpy-compile-variables.xml',
    help="Output an ERDDAP XML blurb or all the variables. ",
)
def compile(**kwargs):
    """Compile NetCDF files variables and variables attributes."""
    variables(**kwargs)


if __name__ == "__main__":
    compile()
