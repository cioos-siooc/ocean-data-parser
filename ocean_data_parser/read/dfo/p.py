"""
P-files is a file format used by the DFO NewfoundLand office.
"""

import logging
import re

import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)

global_attributes = {}
variables_attributes = {
    "pres": {
        "long_name": "Pressure",
        "units": "dbar",
        "standard_name": "sea_water_pressure",
    }
}


def _parse_channel_stats(lines: list) -> dict:
    if not lines:
        return {}
    read_stats = re.compile(
        "# span\s+(?P<name>\w+)\s\=\s*(?P<min>[+-\.\d]+),\s+(?P<max>[+-\.\d]+)"
    )
    spans = [read_stats.search(line).groupdict() for line in lines if "span" in line]
    # Convert it to cf standard
    return {
        item["name"]: {"actual_range": [item["min"], item["max"]]} for item in spans
    }


def _parse_history(lines: list) -> dict:
    return {}


def parser(file: str, encoding="UTF-8") -> xr.Dataset:
    """Convert P-File to an xarray Dataset object

    Args:
        file (str): Path to pfile to parse

    Returns:
        xr.Dataset
    """
    line = None
    header = {}
    section = None
    with open(file, encoding=encoding) as file_handle:
        # Read the four first lines to extract the information
        metadata_lines = [file_handle.readline() for _ in range(4)]
        original_header = metadata_lines
        while file_handle:
            previous_line, line = line, file_handle.readline()
            if "-- DATA --" in line:
                break
            original_header += [line]

            # search section specific
            new_section = re.search("-- ([\w\s]+) -->", line)
            section = new_section[1] if new_section else section
            if section and "-- END --" in line:
                section = None
            if section is None:
                continue
            elif section not in header:
                header[section] = []

            header[section] += [line]

            # TODO: Parse header section to a python dictionary
        # Read data section
        df = pd.read_fwf(file_handle)

    # Get column names and define data types
    df.columns = re.split("\s+", previous_line.strip())
    df = df.astype({col: int if col == "scan" else float for col in df})

    # Convert dataframe to an xarray and populate information
    ds = df.to_xarray()
    ds.attrs.update(global_attributes)
    ds.attrs["original_header"] = "\n".join(original_header)
    ds.attrs['history'] = header.get('HISTORY')
    # TODO bring more attributes from the file header

    # Populate variable attributes
    variables_span = _parse_channel_stats(header.get("CHANNEL STATS"))
    for var in ds:
        ds[var].attrs.update(
            {**variables_attributes.get(var, {}), **variables_span.get(var, {})}
        )
        if var not in variables_attributes:
            logger.warning("unknow variable %s", var)
    return ds
