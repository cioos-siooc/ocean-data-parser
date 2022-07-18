import re
import logging

import pandas as pd
from .utils import standardize_dateset

logger = logging.getLogger(__name__)

string_attributes = ["Cruise_Number", "Cruise_Name", "Station"]


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
    if name in string_attributes:
        return value
    elif re.match(r"\d\d-\w\w\w-\d\d\d\d \d\d\:\d\d\:\d\d\.\d+", value):
        return pd.to_datetime(value, utc=(name and "utc" in name))
    elif re.match(r"^-{0,1}\d+\.\d+$", value):
        return float(value)
    elif re.match(r"^-{0,1}\d+$", value):
        return int(value)
    else:
        return value


def int_format(path, encoding="Windows-1252"):
    metadata = {}
    line = "%"
    with open(path, encoding=encoding) as f:
        # Parse header
        while line.startswith("%"):
            line = f.readline()
            line = line.replace("\n", "")
            if not line.startswith("%"):
                break
            elif line in ("%\n"):
                continue

            key, value = line[1:].split(":", 1)
            metadata[key.strip()] = value.strip()

        # Parse data
        column_names = [item for item in line.split(" ") if item]

        # skip ----- line
        delimiter_line = f.readline()
        if not re.match(r"[\s\-]", delimiter_line):
            logger.error("Delimiter line below the column names isn't the expected one")

        df = pd.read_csv(f, sep=r"\s+", names=column_names)

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

        # Standardize metadata
        metadata = {
            _standardize_attribute_name(name): _standardize_attribute_value(
                value, name=name
            )
            for name, value in metadata.items()
        }

        # Add Global attributes
        ds.attrs = metadata
        for var in ds:
            ds[var].attrs = variables[var]

        # TODO add vocabulary
        ds = standardize_dateset(ds)
        return ds
