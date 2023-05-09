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
    input_dir=None,
    file_regex="**/*.nc",
    groupby=None,
    table_output=None,
    erddap_xml_output=True,
):
    """
    Search any netcdf files within the directory and its subdirectories,
      extract all the variables and their attributes and compile the whole list
    """

    def _get_var_attrs(var, **kwargs):
        return dict(
            file=file, variable=var, dtype=ds[var].dtype, **ds[var].attrs, **kwargs
        )

    groupby = (
        [
            "variable",
            "units",
            "standard_name",
            "sdn_parameter_urn",
            "sdn_parameter_name",
        ]
        if groupby is None
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
    if table_output is None:
        print(df_vars_grouped.to_markdown())
    elif table_output.endswith("md"):
        df_vars_grouped.to_markdown(table_output)
    elif table_output.endswith("csv"):
        df_vars_grouped.to_csv(table_output)

    # Gernerate
    if erddap_xml_output:
        erddap_xml = []
        for id, row in df_vars_unique.iterrows():
            attrs = row.dropna().to_dict()
            file = attrs.pop("file")
            var = attrs.pop("variable")
            dtype = get_erddap_dtype(attrs.pop("dtype", "String"))
            erddap_xml += [get_erddap_xml_variable(var, var, dtype, attrs)]
        erddap_xml = "/n".join(erddap_xml)
        if erddap_xml_output == True:
            print(erddap_xml)
        else:
            with open(erddap_xml_output, "w") as file_handle:
                file_handle.write(erddap_xml)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Ocean Data Parser NetCDF Compiler")
    parser.add_argument(
        "type", type=str, help="Which component of netcdfs to compile? [variables]"
    )
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
    args = vars(parser.parse_args())
    compile_type = args.pop("type")
    if "variable" in compile_type:
        output_path = args.pop("output")
        if output_path:
            args["table_output"] = output_path
            args["erddap_xml_output"] = output_path.rsplit(".", 1)[0] + ".xml"
        variables(**args)
