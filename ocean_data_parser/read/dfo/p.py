"""
P-files is a file format used by the DFO NewfoundLand office.
"""

import logging
import re

import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)

# TODO add global and variable attributes. Variable attributes could be move to an external file
global_attributes = {}
variables_attributes = {
    "pres": {
        "long_name": "Pressure",
        "units": "dbar",
        "standard_name": "sea_water_pressure",
    }
}


def _get_dtype(var):
    return int if var == "scan" else float


def _parse_channel_stats(lines: list) -> dict:
    def _get_range(attrs: dict) -> tuple:
        """Convert range to the variable dtype"""
        dtype = _get_dtype(attrs["name"])

        # Use int(float(x)) method because the integers have decimals
        func = (lambda x: int(float(x))) if dtype == int else float
        return tuple(
            map(
                func,
                [attrs["min"], attrs["max"]],
            )
        )

    if not lines:
        return {}
    read_stats = re.compile(
        r"# span\s+(?P<name>\w+)\s\=\s*(?P<min>[+-\.\d]+),\s+(?P<max>[+-\.\d]+)"
    )
    spans = [read_stats.search(line).groupdict() for line in lines if "span" in line]
    # Convert it to cf standard
    return {item["name"]: {"actual_range": _get_range(item)} for item in spans}


def _parse_history(lines: list) -> dict:
    # TODO convert history to cf format: 2022-02-02T00:00:00Z - ...
    return {}


# TODO Name every fields present within the file header
""" from 56001001.p2022
NAFC_Y2K_HEADER
56001001  47 32.80 -052 35.20 2022-04-10 14:06 0176 S1460 001 V S27-01         1
56001001 002260  8.00 A 13 #PTCSMOFLHXAW-------            D 000 0001 0173 000 4
56001001 7 08 02    0999.1 003.8       08 01 18 10 01                          8
-- CHANNEL STATS -->
"""
metatadata_items = (
    (
        "56001001",
        "lat_deg",
        "lat_min",
        "lon_deg",
        "lon_min",
        "date",
        "time",
        "?0176",
        "?S1460",
        "?V",
        "?S27-01",
        "1",
    ),
    (
        "56001001",
        "?002260",
        "?8.00",
        "?A",
        "?13",
        "?#PTCSMOFLHXAW-------",
        "?D",
        "?000",
        "?0001",
        "?0173",
        "?000",
        "?4",
    ),
    (
        "56001001",
        "?7",
        "?08",
        "?02",
        "?0999.1",
        "?003.8",
        "?08",
        "?01",
        "?18",
        "?10",
        "?01",
        "?8",
    ),
)


def _parse_metadata_header(header_lines: list) -> dict:
    """Parse the three metadata lines present within the p files"""
    assert len(header_lines) == 3, "expected 3 separate lines"
    metadata = {}
    for names, values in zip(metatadata_items, header_lines):
        metadata.update(**dict(zip(names, re.split("\s+", values))))

    # Transform some of the fields to main standards
    metadata["datetime"] = pd.to_datetime(
        f"{metadata.pop('date')} {metadata.pop('time')}", format="%Y-%m-%d %H:%M"
    )  # UTC?
    metadata["latitude"] = (
        float(metadata.pop("lat_deg")) + float(metadata.pop("lat_min")) / 60
    )
    metadata["longitude"] = (
        float(metadata.pop("lon_deg")) + float(metadata.pop("lon_min")) / 60
    )
    return metadata


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
        original_header = [file_handle.readline() for _ in range(4)]
        metadata_lines = original_header[:4]
        while file_handle:
            previous_line, line = line, file_handle.readline()
            if "-- DATA --" in line:
                break
            original_header += [line]

            # search section specific
            new_section = re.search(r"-- ([\w\s]+) -->", line)
            section = new_section[1] if new_section else section
            if section and "-- END --" in line:
                section = None
            if section is None:
                continue
            elif section not in header:
                header[section] = []

            header[section] += [line]

        # Read data section
        df = pd.read_fwf(file_handle)

    # Get column names and define data types
    df.columns = re.split(r"\s+", previous_line.strip())
    df = df.astype({col: _get_dtype(col) for col in df})

    # Convert dataframe to an xarray and populate information
    ds = df.to_xarray()
    ds.attrs.update(global_attributes)
    ds.attrs.update(_parse_metadata_header(metadata_lines[1:]))
    ds.attrs["original_header"] = "\n".join(original_header)
    ds.attrs["history"] = header.get("HISTORY")
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
