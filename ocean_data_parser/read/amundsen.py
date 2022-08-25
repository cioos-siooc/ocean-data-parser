"""Module use to handle int file format generated historically ArcticNet and the Amundsen inc."""
__version__ = "0.1.0"

import re
import logging
import os
import json

import pandas as pd
from gsw import z_from_p
from .utils import standardize_dateset, get_history_handler

logger = logging.getLogger(__name__)
string_attributes = ["Cruise_Number", "Cruise_Name", "Station"]
reference_vocabulary_path = os.path.join(
    os.path.dirname(__file__), "vocabularies", "amundsen_variable_attributes.json"
)

# Read vocabulary file
with open(reference_vocabulary_path, encoding="UTF-8") as vocabulary_file:
    amundsen_variable_attributes = json.load(vocabulary_file)

default_global_attributes = {"unknown_variables_information": "", "history": ""}


def _standardize_attribute_name(name: str) -> str:
    """
    Standardize attribute names to
        - All lower case
        - All symbols " []" are replaced by underscores
        - Trailing underscores removed.
    Args:
        name (string): original attribute name

    Returns (string): converted attribute name
    """
    formatted_name = re.sub(r"[\s\[\]]+", "_", name.lower())
    formatted_name = re.sub("_$", "", formatted_name)
    return formatted_name


def _standardize_attribute_value(value: str, name: str = None):
    """Cast attribute value to the appropriate format

    Args:
        value (string): [description]
        name (string, optional): [description]. Defaults to None.

    Returns:
        [str,float,int,pd.Timestamp]: cast attribute value according to the right format.
    """
    if name in string_attributes or not isinstance(value, str):
        return value
    elif re.match(r"\d\d-\w\w\w-\d\d\d\d \d\d\:\d\d\:\d\d\.\d+", value):
        return pd.to_datetime(value, utc=(name and "utc" in name))
    elif re.match(r"^-{0,1}\d+\.\d+$", value):
        return float(value)
    elif re.match(r"^-{0,1}\d+$", value):
        return int(value)
    else:
        return value


def date_parser(time_str):
    """Amundsen INT Date + Hour time parser"""
    if time_str.endswith(":60"):
        time_str = time_str.replace(":60", ":00")
        add_time = pd.Timedelta(seconds=60)
    else:
        add_time = pd.Timedelta(seconds=0)

    return (pd.to_datetime(time_str) + add_time).to_pydatetime()


def int_format(
    path, encoding="Windows-1252", map_to_vocabulary=True, generate_depth=True
):
    """Parse INT format developed and distributed by ArcticNet
    and the Amundsen groups over the years."""
    nc_logger, nc_handler = get_history_handler()
    logger.addHandler(nc_handler)

    logger.info(
        "Convert INT file format with python package ocean_data_parser.amundsen.int_format V%s",
        __version__,
    )
    metadata = default_global_attributes.copy()

    # Ignore info.int files
    if path.endswith("_info.int"):
        logger.warning("Ignore *_info.int files: %s", path)
        return

    logger.debug("Read %s", path)
    with open(path, encoding=encoding) as file:
        # Parse header
        for line in file:
            line = line.replace("\n", "")
            if re.match(r"^%\s*$", line) or not line:
                continue
            elif line and not re.match(r"\s*%", line) and line[0] == " ":
                last_line = line
                break
            elif ":" in line:
                key, value = line.strip()[1:].split(":", 1)
                metadata[key.strip()] = value.strip()
            elif re.match(r"% .* \[.+\]", line):
                logger.warning(
                    "Unknown variable name will be saved to unknown_variables_information: %s",
                    line,
                )
                metadata["unknown_variables_information"] += line + "\n"

            else:
                logger.warning("Unknown line format: %s", line)

        # Review metadata
        if metadata == default_global_attributes:
            logger.warning("No metadata was captured in the header of the INT file.")

        # Parse Columne Header by capital letters
        column_name_line = last_line
        delimiter_line = file.readline()
        if not re.match(r"^[\s\-]+$", delimiter_line):
            logger.error("Delimiter line below the column names isn't the expected one")

        # Parse column names based on delimiter line below
        delimited_segments = re.findall(r"\s*\-+", delimiter_line)
        start_segment = 0
        column_names = []
        for segment in delimited_segments:
            column_names += [
                column_name_line[start_segment : start_segment + len(segment)].strip()
            ]
            start_segment = start_segment + len(segment)

        # Time variable
        if "Date" in column_names and "Hour" in column_names:
            logger.info("Generate a time variable from Date and Hour variables")
            parse_dates = {"time": ["Date", "Hour"]}
        else:
            parse_dates = None

        # Parse data
        df = pd.read_csv(
            file,
            sep=r"\s+",
            names=column_names,
            parse_dates=parse_dates,
            date_parser=date_parser,
        )

        # Sort column attributes
        variables = {
            column: re.search(
                r"(?P<long_name>[^\[]+)(\[(?P<units>.*)\]){0,1}",
                metadata.pop(column[0].upper() + column[1:]),
            ).groupdict()
            if column[0].upper() + column[1:] in metadata
            else {}
            for column in df
        }

        # Convert to xarray object
        ds = df.to_xarray()

        # Standardize global attributes
        metadata = {
            _standardize_attribute_name(name): _standardize_attribute_value(
                value, name=name
            )
            for name, value in metadata.items()
        }
        ds.attrs = metadata

        # Generate instrument_depth variable
        if (
            generate_depth
            and "Pres" in ds
            and ("Lat" in ds or "initial_latitude_deg" in ds.attrs)
        ):
            latitude = (
                ds["Latitude"] if "Lat" in ds else ds.attrs["initial_latitude_deg"]
            )
            logger.info(
                "Generate instrument_depth from TEOS-10: -1 * gsw.z_from_p(ds['Pres'], %s)",
                "ds['Lat']" if "Lat" in ds else "ds.attrs['initial_latitude_deg']",
            )
            ds["instrument_depth"] = -z_from_p(ds["Pres"], latitude)

        # Map varibles to vocabulary
        variables_to_rename = {}
        for var in ds:
            if var not in variables:
                continue

            ds[var].attrs = variables[var]
            if "long_name" in ds[var].attrs:
                ds[var].attrs["long_name"] = ds[var].attrs["long_name"].strip()

            # Include variable attributes from the vocabulary
            if not map_to_vocabulary:
                continue
            elif var not in amundsen_variable_attributes:
                logger.warning("No vocabulary is available for variable '%s'", var)
                continue

            # Match vocabulary
            var_units = ds[var].attrs.get("units")
            for item in amundsen_variable_attributes[var]:
                accepted_units = item.get("accepted_units")
                if (
                    var_units is None  # Consider first if no units
                    or var_units == item.get("units")
                    or (accepted_units and re.match(accepted_units, var_units))
                ):
                    if "rename" in item:
                        variables_to_rename[var] = item["rename"]

                    ds[var].attrs = {
                        key: value
                        for key, value in item.items()
                        if key not in ["accepted_units", "rename"]
                    }
                    break
            else:
                logger.warning(
                    "No Vocabulary available for %s: %s", var, str(ds[var].attrs)
                )

        # Review rename variables
        already_existing_variables = {
            var: rename for var, rename in variables_to_rename.items() if rename in ds
        }
        if already_existing_variables:
            logger.error(
                "Can't rename variable %s since it already exist",
                already_existing_variables,
            )

        if variables_to_rename:
            logger.info("Rename variables: %s", variables_to_rename)
            ds = ds.rename(variables_to_rename)

        # Generate history
        ds.attrs["history"] += nc_logger.getvalue()

        # Standardize dataset to be compatible with ERDDAP and NetCDF Classic
        ds = standardize_dateset(ds)
        return ds
