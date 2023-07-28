"""
# ElectricBlue
<https://electricblue.eu/envloggers>

ElectricBlue is a non-profit technology transfer startup creating research-oriented solutions for the scientific community
"""
import logging
import re

import pandas as pd
import xarray

from ocean_data_parser.parsers.utils import (
    rename_variables_to_valid_netcdf,
    standardize_dataset,
)

logger = logging.getLogger(__name__)

default_global_attribute = {
    "instrument_manufacturer": "ElectricBlue",
    "instrument_manufacturer_webpage": "https://electricblue.eu/",
    "source": None,
    "source_file_header": "",
}
default_variable_attributes = {
    "latitude": {
        "long_name": "Latitude",
        "units": "degrees_east",
        "standard_name": "latitude",
    },
    "longitude": {
        "long_name": "Longitude",
        "units": "degrees_north",
        "standard_name": "longitude",
    },
    "time": {"long_name": "Measurement Time", "standard_name": "time"},
    "temp": {"long_name": "Temperature", "standard_name": "sea_water_temperature"},
}


def csv(
    path: str,
    encoding: str = "utf-8",
) -> xarray.Dataset:
    """ElectricBlue csv data format parser

    Args:
        path (str): path to the csv file to parse
        encoding (str='UTF-8', optional): file encoding
    Returns:
        dataset: xarray dataset
    """
    with open(path, encoding=encoding) as f:
        line = True
        metadata = default_global_attribute
        metadata["source_file"] = path

        while line:
            line = f.readline()
            metadata["source_file_header"] += line
            line = line.strip()

            if re.match(r"^[-,\s]+$", line):
                continue
            elif line.startswith("time,"):
                break
            else:
                items = line.split(", ", 1)
                key = items[0]
                value = items[1] if len(items) == 2 else ""

                attr = re.sub(r"[\s\[\]\(\)]+", "_", key.lower())

                # cast value
                if re.match(r"^[+-]*\d+$", value):
                    value = int(value)
                elif re.match(r"^[+-]*\d+\.\d+$", value):
                    value = float(value)

                metadata[attr] = value

        columns = line.split(",")
        df = pd.read_csv(
            f,
            sep=",",
            header=None,
            names=columns,
            parse_dates=[0],
            date_parser=lambda x: pd.to_datetime(
                x + metadata.pop("time_zone"), utc=True
            ),
        )
        if len(df) != metadata["samples"]:
            logger.warning(
                "Parsed data samples=%s do not match expected samples=%s",
                str(len(df)),
                metadata["samples"],
            )

        # Convert to xarray dataset
        ds = df.to_xarray()

        # Global attributes
        ds.attrs = metadata
        ds.attrs.update(
            {
                "instrument_type": ds.attrs.get("envlogger_version"),
                "instrument_sn": ds.attrs.get("serial_number"),
            }
        )
        ds["latitude"] = ds.attrs["lat"]
        ds["longitude"] = ds.attrs["long"]

        # Variables attributes
        for var in ds:
            if var in default_variable_attributes:
                ds[var].attrs = default_variable_attributes[var]
        ds["temp"].attrs["units"] = ds.attrs.pop("temperature")
        ds = standardize_dataset(ds)
        return ds


def log_csv(
    path: str, encoding: str = "UTF-8", rename_variables: bool = True
) -> xarray.Dataset:
    df = pd.read_csv(path, encoding=encoding, parse_dates=True, index_col=["time"])
    ds = df.to_xarray()
    # add default attributes
    ds.attrs.update({**default_global_attribute, "source": path})
    ds = standardize_dataset(ds)

    # Rename variables to be compatible with NetCDF
    if rename_variables:
        ds = rename_variables_to_valid_netcdf(ds)
    return ds
