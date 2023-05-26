import logging
from pathlib import Path

import click
import pandas as pd
import xarray as xr
from tqdm import tqdm

logger = logging.getLogger(__name__)


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


def variables(
    input_dir: str = ".",
    file_regex: str = "**/*.nc",
    groupby: list = None,
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

    def _get_var_attrs(var, **kwargs):
        return dict(
            file=file, variable=var, dtype=ds[var].dtype, **ds[var].attrs, **kwargs
        )

    if isinstance(groupby, str):
        groupby = groupby.split(",")
    groupby = (
        [
            "variable",
            "units",
            "standard_name",
            "sdn_parameter_urn",
            "sdn_parameter_name",
        ]
        if not groupby
        else groupby
    )

    # Get file list
    input_dir = Path(input_dir or ".")
    files = list(input_dir.glob(file_regex))
    if not files:
        logger.info("No netcdf files available")
        return

    # Compile variable list
    variables = []
    for file in tqdm(files, unit="file", desc="Compile NetCDF variables"):
        ds = xr.open_dataset(file)
        variables += [_get_var_attrs(var, coords=True) for var in ds.coords]
        variables += [_get_var_attrs(var) for var in ds]

    # Generate DataFrame
    df_vars = pd.DataFrame(variables)
    logger.debug("columns available: %s", df_vars.columns)
    df_vars_grouped = (
        df_vars.groupby(
            groupby,
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
    "--input_dir",
    "-i",
    default=".",
    type=click.Path(exists=True),
    help="Top directory from where to look for netCDFs",
)
@click.option(
    "--file_regex", "-f", default="**/*.nc", type=str, help="File search expression."
)
@click.option(
    "--groupby",
    "-g",
    default=None,
    type=str,
    help="Comma separated list of attributes to regroup variables by.",
)
@click.option(
    "--output_table",
    "-t",
    default=None,
    help="Output table result to console(default), csv (**/*.csv) or markdown(**/*.md).",
)
@click.option(
    "--output_erddap_xml",
    "-xml",
    default=None,
    help="Output an ERDDAP XML blurb or all the variables. ",
)
def cli_variables(**kwargs):
    """Compile NetCDF files variables and variables attributes."""
    kwargs["groupby"] = kwargs["groupby"].split(",") if kwargs["groupby"] else None
    variables(**kwargs)


if __name__ == "__main__":
    cli_variables()
