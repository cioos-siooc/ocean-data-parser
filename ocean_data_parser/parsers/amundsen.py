"""
# Amundsen
<https://arcticnet.ulaval.ca/>
<https://amundsenscience.com/>

Historically ArcticNet and the Amundsen Siences.
"""
import json
import logging
import re

import pandas as pd
import xarray as xr
from gsw import z_from_p

from ocean_data_parser._version import __version__
from ocean_data_parser.parsers.utils import get_history_handler, standardize_dataset
from ocean_data_parser.vocabularies.load import amundsen_vocabulary

logger = logging.getLogger(__name__)
string_attributes = ["Cruise_Number", "Cruise_Name", "Station"]
amundsen_variable_attributes = amundsen_vocabulary()

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
    elif re.match(r"\d\d-\w\w\w-\d\d\d\d \d\d\:\d\d\:\d\d", value):
        return pd.to_datetime(value, utc=(name and "utc" in name))
    elif re.match(r"^-{0,1}\d+\.\d+$", value):
        return float(value)
    elif re.match(r"^-{0,1}\d+$", value):
        return int(value)
    else:
        return value


def int_format(
    path: str,
    encoding: str = "Windows-1252",
    map_to_vocabulary: bool = True,
    generate_depth: bool = True,
) -> xr.Dataset:
    """Parse Amundsen INT format.

    The Amundsen INT format is a tabular format

    Args:
        path (str): file path to parse.
        encoding (str, optional): File encoding. Defaults to "Windows-1252".
        map_to_vocabulary (bool, optional): Rename variables to vocabulary. Defaults to True.
        generate_depth (bool, optional): Generate depth variable. Defaults to True.

    Returns:
        xr.Dataset: xarray compliant with CF-1.6
    """
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
            elif line == "% Fluorescence [ug/L]":
                metadata["Fluo"] = "Fluorescence [ug/L]"
            elif line == "% Conservative Temperature (TEOS-10) [deg C]":
                metadata["CONT"] = "Conservative Temperature (TEOS-10) [deg C]"
            elif line == "% In situ density TEOS10 ((s, t, p) - 1000) [kg/m^3]":
                metadata["D_CT"] = "In situ density TEOS10 ((s, t, p) - 1000) [kg/m^3]"
            elif line == "% Potential density TEOS10 ((s, t, 0) - 1000) [kg/m^3]":
                metadata[
                    "D0CT"
                ] = "Potential density TEOS10 ((s, t, 0) - 1000) [kg/m^3]"
            elif line == "% Potential density TEOS10 (s, t, 0) [kg/m^3]":
                metadata["D0CT"] = "Potential density TEOS10 (s, t, 0) [kg/m^3]"
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

        # Parse data
        df = pd.read_csv(
            file,
            sep=r"\s+",
            names=column_names,
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
        if "Date" in df and "Hour" in df:
            is_60 = df["Hour"].str.contains(":60$")
            df.loc[is_60, "Hour"] = df.loc[is_60, "Hour"].str.replace(
                ":60$", ":00", regex=True
            )
            df["time"] = pd.to_datetime(df["Date"] + "T" + df["Hour"], utc=True)
            df.loc[is_60, "time"] += pd.Timedelta(seconds=60)

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
        pressure = [var for var in ds if var in ("Pres", "PRES")]
        if (
            generate_depth
            and pressure
            and ("Lat" in ds or "initial_latitude_deg" in ds.attrs)
        ):
            latitude = (
                ds["Latitude"] if "Lat" in ds else ds.attrs["initial_latitude_deg"]
            )
            logger.info(
                "Generate instrument_depth from TEOS-10: -1 * gsw.z_from_p(ds['Pres'], %s)",
                "ds['Lat']" if "Lat" in ds else "ds.attrs['initial_latitude_deg']",
            )
            ds["instrument_depth"] = -z_from_p(ds[pressure[0]], latitude)

        # Map variables to vocabulary
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
                    or (accepted_units and re.fullmatch(accepted_units, var_units))
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
        ds = standardize_dataset(ds)
        return ds
