"""[ElectricBlue](https://electricblue.eu/envloggers)
is a non-profit technology transfer startup creating
research-oriented solutions for the scientific community.
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

GLOBAL_ATTRIBUTES = {
    "instrument_manufacturer": "ElectricBlue",
    "instrument_manufacturer_webpage": "https://electricblue.eu/",
    "source": None,
    "source_file_header": "",
}
VARIABLE_ATTRIBUTES = {
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
    """ElectricBlue csv data format parser.

    Args:
        path (str): path to the csv file to parse
        encoding (str='UTF-8', optional): file encoding

    Returns:
        xarray.Dataset
    """
    with open(path, encoding=encoding) as f:
        line = True
        metadata = GLOBAL_ATTRIBUTES
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

                attr = re.sub(r"[\s\[\]\(\)\-]+", "_", key.lower())
                attr = re.sub(r"__+", "_", attr)
                attr = re.sub(r"_$", "", attr)

                # cast value
                if re.match(r"^[+-]*\d+$", value):
                    value = int(value)
                elif re.match(r"^[+-]*\d+\.\d+$", value):
                    value = float(value)

                metadata[attr] = value

        columns = line.split(",")
        time_zone = metadata.pop("time_zone")
        df = pd.read_csv(
            f,
            sep=",",
            header=None,
            names=columns,
            converters={0: lambda x: pd.to_datetime(x + time_zone, utc=True)},
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
            if var in VARIABLE_ATTRIBUTES:
                ds[var].attrs = VARIABLE_ATTRIBUTES[var]
        ds["temp"].attrs["units"] = ds.attrs.pop("temperature")
        ds = standardize_dataset(ds)
        return ds


def log_csv(
    path: str, encoding: str = "UTF-8", rename_variables: bool = True
) -> xarray.Dataset:
    """Parse ElectricBlue log csv file.

    Args:
        path (str): path to the csv file
        encoding (str, optional): File encoding. Defaults to "UTF-8".
        rename_variables (bool, optional): Rename variables to
            valid NetCDF names. Defaults to True.

    Returns:
        xarray.Dataset
    """
    df = pd.read_csv(path, encoding=encoding, parse_dates=True, index_col=["time"])
    ds = df.to_xarray()
    # add default attributes
    ds.attrs.update({**GLOBAL_ATTRIBUTES, "source": path})
    ds = standardize_dataset(ds)

    # Rename variables to be compatible with NetCDF
    if rename_variables:
        ds = rename_variables_to_valid_netcdf(ds)
    return ds
