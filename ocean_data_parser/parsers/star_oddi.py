"""[Star-Oddi](https://www.star-oddi.com/).

Star-oddi is a company that specializes in manufacturing and providing data
loggers and sensors for oceanographic research. Their DAT files contain recorded
data from various oceanographic parameters such as temperature, salinity, conductivity, and sound velocity.
"""

import logging
import re

import pandas as pd
import xarray

logger = logging.getLogger(__name__)

DEFAULT_GLOBAL_ATTRIBUTES = {"instrument_manufacturer": "Star-Oddi", "source": None}

VARIABLES_ATTRIBUTES = {
    "temperature": {
        "long_name": "Temperature",
        "standard_name": "sea_water_temperature",
    },
    "salinity": {
        "long_name": "Salinity",
        "standard_name": "sea_water_practical_salinity",
    },
    "conductivity": {
        "long_name": "Temperature",
        "standard_name": "sea_water_conductivity",
    },
    "sound_velocity": {
        "long_name": "Sound Velocity",
        "standard_name": "speed_of_sound_in_sea_water",
    },
    "pressure": {
        "long_name": "Pressure",
        "standard_name": "sea_water_pressure",
    },
}


def DAT(path: str, encoding: str = "cp1252") -> xarray.Dataset:  # noqa
    """Deprecated Star-Oddi DAT files parser."""
    logger.warning("Function name DAT is deprecated, use dat instead.")
    return dat(path, encoding)


def dat(path: str, encoding: str = "cp1252") -> xarray.Dataset:
    """Parse Star-Oddi DAT files.

    Args:
        path (str): DAT file path
        encoding (str, optional): Encoding used. Defaults to "cp1252".

    Returns:
        xarray.Dataset: Dataset
    """

    def _standardize_attributes(item):
        item = re.sub(r"[\.\:]", "", item.strip().lower())
        return re.sub(r"\s", "_", item)

    metadata = {}
    variables = {}
    original_header = ""
    with open(path, encoding=encoding) as f:
        line = "#"

        # Loop through the header lines
        while line.startswith("#"):
            line = f.readline()
            original_header += line
            if not line.startswith("#"):
                break

            _, attr, value = line.strip().split("\t", 2)
            if attr.strip() == "Axis":
                axis = line.strip().split("\t")
                name = re.search(r"(?P<name>[^\(]+)\((?P<units>.+)\)", axis[3])

                variables[_standardize_attributes(name["name"])] = {
                    "long_name": name["name"],
                    "units": name["units"],
                }
            elif attr.strip() == "Series":
                pass
            else:
                metadata[_standardize_attributes(attr)] = value.strip()

        # Check if date & time format is supported
        if (
            metadata["date_&_time"] == "1"
            and metadata["date_def"] == "dd.mm.yyyy	."
            and metadata["time_def"] == ":"
        ):
            date_format = "%d.%m.%Y\t%H:%M:%S"
            variables = {**{"time": {}}, **variables}
        elif (
            metadata["date_&_time"] == "1"
            and metadata["date_def"] == "dd-mm-yyyy\t-"
            and metadata["time_def"] == ":"
        ):
            date_format = "%d-%m-%Y\t%H:%M:%S"
            variables = {**{"time": {}}, **variables}
        elif metadata["date_&_time"] == "1":
            raise ValueError("Date & Time format is not supported")

        # TODO parse recorder info
        # TODO rename attributes to cf standard
        # TODO parse data line to review time range and n_records
        # TODO add some logging info

        # Parse data
        df = pd.read_csv(
            f,
            header=None,
            sep="\t",
            decimal=metadata.pop("decimal_point"),
            names=variables.keys(),
            parse_dates=["time"],
            date_format=date_format,
            dayfirst=True,
        )
        if "time" in df:
            df = df.set_index(["time"])

        # Parse data section of header
        n_records, start_time, end_time = metadata.pop("data").split("\t")

        if int(n_records) != len(df):
            logger.warning(
                "Length of data retrieved (=%s) does not match the expected length from the header (=%s).",
                len(df),
                int(n_records) - 1,
            )
        # Convert to xarray object and add related metadata
        ds = df.to_xarray()
        ds.attrs = {
            **DEFAULT_GLOBAL_ATTRIBUTES,
            "source": path,
            **dict(
                zip(
                    ("instrument_model", "instrument_serial_number"),
                    metadata.pop("recorder").split("\t")[1:],
                )
            ),
            **dict(
                zip(
                    ("software", "software_version"),
                    metadata.pop("version").split("\t"),
                )
            ),
            "n_records": n_records,
            "start_time": pd.to_datetime(
                start_time, format=date_format, dayfirst=True
            ).isoformat(),
            "end_time": pd.to_datetime(
                end_time, format=date_format, dayfirst=True
            ).isoformat(),
            "date_created": pd.to_datetime(
                metadata.pop("created"), format=date_format, dayfirst=True
            ).isoformat(),
            "original_file_header": original_header,
        }
        # Add variable attributes
        for var in ds:
            if var not in VARIABLES_ATTRIBUTES:
                logger.warning("Unknown variable %s", var)
            ds[var].attrs = {**variables[var], **VARIABLES_ATTRIBUTES.get(var, {})}
        return ds
