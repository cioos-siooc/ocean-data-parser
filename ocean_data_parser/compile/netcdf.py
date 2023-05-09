import argparse
import logging
from pathlib import Path

import pandas as pd
import xarray as xr
from tqdm import tqdm

logger = logging.getLogger(__name__)


def get_erddap_dtype(dtype):
    if "float" in str(dtype):
        return "float"
    elif "int" in str(dtype):
        return "int"
    elif dtype in (str, object):
        return "String"
    else:
        logger.error("Unknown erddap dtype=%s mapping")
        return dtype


def get_erddap_xml_variable(variable, destination, type, attrs):
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
    input_dir=None, file_regex="**/*.nc", groupby=None, output=None, get_erddap_xml=True
):
    """Search any netcdf files within the directory and its subdirectories, extract all the variables and their attributes and compile the whole list"""

    groupby = ["variable", "units"] if groupby is None else groupby

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
        for var in ds:
            variables += [
                dict(file=file, variable=var, dtype=ds[var].dtype, **ds[var].attrs)
            ]

    # Generate DataFrame
    df_vars = pd.DataFrame(variables)
    df_vars_grouped = (
        df_vars.groupby(["variable", "units"]).agg({"file": "count"})
    ).reset_index()
    df_vars_unique = df_vars.drop_duplicates(subset=["variable", "units"], keep="first")

    # Generate output
    logger.info("Unique Variable list")
    if output is None:
        print(df_vars_grouped.to_markdown())
    elif output.endswith("md"):
        df_vars_grouped.to_markdown(output)
    elif output.endswith("csv"):
        df_vars_grouped.to_csv(output)

    # Gernerate
    if get_erddap_xml:
        for id, row in df_vars_unique.iterrows():
            attrs = row.dropna().to_dict()
            file = attrs.pop("file")
            var = attrs.pop("variable")
            dtype = get_erddap_dtype(attrs.pop("dtype", "String"))
            print(get_erddap_xml_variable(var, var, dtype, attrs))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Ocean Data Parser NetCDF Compiler")
    parser.add_argument("-i", "--input_dir", type=str, help="input directory")
    parser.add_argument(
        "--file_regex", type=str, help="file glob type search", default="**/*.nc"
    )
    parser.add_argument(
        "--groupby",
        type=str,
        help="comma delimited list of attributes,variables,file to group by",
    )
    parser.add_argument(
        "-o", "--output", type=str, help="Output type/path to present results"
    )
    args = parser.parse_args()
    # start_dir = "/Users/jessybarrette/repo/ocean-data-parser-start/output"
    variables(**vars(args))
