import os
from glob import glob
from multiprocessing import Pool

import click
import pandas as pd
import xarray as xr
from loguru import logger
from tqdm import tqdm


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
    multiprocessing: int = None,
):
    """Compile NetCDF files variables and variables attributes.

    Args:
        input (str, optional): Glob expression of input files. Defaults to "**/*.nc".
        exclude (str, optional): Glob expression of files to exclude.
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
        multiprocessing (int, optional): Number of processors to use.
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
    tqdm_kwargs = dict(unit="file", desc="Compile NetCDF variables", total=len(files))
    if multiprocessing == 1:
        variables = [_get_netcdf_variables(file) for file in tqdm(files, **tqdm_kwargs)]
    else:
        with Pool(multiprocessing) as pool:
            variables = list(
                tqdm(pool.imap(_get_netcdf_variables, files), **tqdm_kwargs)
            )
    # Generate DataFrame
    #  unpack variables which is a list of list of variables per files.
    df_vars = pd.DataFrame(sum(variables, []))
    df_vars_grouped = (
        df_vars.groupby(
            groupby.split(","),
            dropna=False,
        ).agg({"file": "count"})
    ).reset_index()
    df_vars_unique = df_vars.drop_duplicates(subset=["variable", "units"], keep="first")

    # Generate output
    if output_table is None:
        logger.info("Generate variable tables:")
        print(df_vars_grouped.to_markdown())
    elif output_table.endswith("md"):
        logger.info("Save variable table to: {}", output_table)
        df_vars_grouped.to_markdown(output_table)
    elif output_table.endswith("csv"):
        logger.info("Save variable table to: {}", output_table)
        df_vars_grouped.to_csv(output_table)

    # Gernerate
    if output_erddap_xml:
        logger.debug("Compile xml")
        erddap_xml = []
        for _, row in df_vars_unique.iterrows():
            attrs = row.dropna().to_dict()
            attrs.pop("file")
            var = attrs.pop("variable")
            dtype = _get_erddap_dtype(attrs.pop("dtype", "String"))
            erddap_xml += [_get_erddap_xml_variable(var, var, dtype, attrs)]
        erddap_xml = "\n".join(erddap_xml)
        if output_erddap_xml in (True, "true", "True", "1"):
            print(erddap_xml)
        else:
            with open(output_erddap_xml, "w") as file_handle:
                file_handle.write(erddap_xml)


@click.command(context_settings={"auto_envvar_prefix": "ODPY_COMPILE"})
@click.option(
    "--input",
    "-i",
    default="**/*.nc",
    show_default=True,
    type=str,
    help="Glob expression of input files",
)
@click.option("--exclude", type=str, help="Glob expression of files to exclude")
@click.option(
    "--groupby",
    default="variable,units,standard_name,sdn_parameter_urn,sdn_parameter_name",
    type=str,
    help="Comma separated list of attributes to regroup variables by.",
    show_default=True,
)
@click.option(
    "--multiprocessing",
    "-m",
    type=click.IntRange(1, os.cpu_count()),
    default=1,
    flag_value=os.cpu_count(),
    is_flag=False,
    help=(
        "Load files in parallele with n processors. "
        "If the option is set as a flag, "
        f"all the processors available (={os.cpu_count()}) will be used."
    ),
    show_default=True,
)
@click.option(
    "--output-table",
    "-t",
    default=None,
    is_flag=False,
    flag_value="odpy-compile-variables.md",
    help="Result table output:\n default to console otherwise output path[csv or md].",
)
@click.option(
    "--output-erddap-xml",
    "--xml",
    default=None,
    is_flag=False,
    flag_value=True,
    help="Output an ERDDAP XML blurb or all the variables. ",
)
@click.option(
    "--show-arguments",
    is_flag=False,
    flag_value="True",
    type=click.Choice(["stop", "True"]),
    default=None,
    help="Print present argument values. If stop, do not run the conversion.",
)
def inspect_variables(**kwargs):
    """Inspect NetCDF files variables and variables attributes."""
    if kwargs.get("show_arguments"):
        click.echo("odpy convert parameter inputs:")
        click.echo("\n".join([f"{key}={value}" for key, value in kwargs.items()]))
        if kwargs["show_arguments"] == "stop":
            return
    kwargs.pop("show_arguments", None)
    variables(**kwargs)


if __name__ == "__main__":
    inspect_variables()
