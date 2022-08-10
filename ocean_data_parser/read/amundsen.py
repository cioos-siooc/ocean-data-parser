import re
import logging
import os
import json

import pandas as pd
from .utils import standardize_dateset

logger = logging.getLogger(__name__)

string_attributes = ["Cruise_Number", "Cruise_Name", "Station"]

reference_vocabulary_path = os.path.join(
    os.path.dirname(__file__), "vocabularies", "amundsen_variable_attributes.json"
)
# Read vocabulary file
with open(reference_vocabulary_path, encoding="UTF-8") as vocabulary_file:
    amundsen_variable_attributes = json.load(vocabulary_file)

    # Make it non case sensitive by lowering all keys
    amundsen_variable_attributes = {
        key.lower(): attrs for key, attrs in amundsen_variable_attributes.items()
    }


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


def int_format(path, encoding="Windows-1252", map_to_vocabulary=False):
    """Parse INT format developed and distributed by ArcticNet
    and the Amundsen groups over the years."""
    metadata = {"unknown": []}
    line = "%"
    logger.warning("Read %s", path)
    with open(path, encoding=encoding) as file:
        # Parse header
        for line in file:
            line = line.replace("\n", "")
            if not re.match(r"\s*%", line) and line:
                break
            elif re.match(r"^%\s*$", line) or not line:
                continue
            elif ":" in line:
                key, value = line.strip()[1:].split(":", 1)
                metadata[key.strip()] = value.strip()
            elif re.match(r"% .* \[.+\]", line):
                logger.warning(
                    "Line with missing variable mapping will be saved in unknown: %s",
                    line,
                )
                metadata["unknown"] += line

            else:
                logger.warning("Unknown line format: %s", line)

        # Parse Columne Header by capital letters
        column_name_line = line
        delimiter_line = file.readline()
        if not re.match(r"[\s\-]", delimiter_line):
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
        df = pd.read_csv(file, sep=r"\s+", names=column_names)

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
            if "long_name" in ds[var].attrs:
                ds[var].attrs["long_name"] = ds[var].attrs["long_name"].title().strip()
            # Include variable attributes from the vocabulary
            if map_to_vocabulary:
                if var.lower() in amundsen_variable_attributes:
                    ds[var].attrs.update(amundsen_variable_attributes[var.lower()])
                else:
                    logger.warning("No vocabulary is available for variable '%s'", var)

        ds = standardize_dateset(ds)
        return ds