import pandas as pd
import re

import logging

logger = logging.getLogger(__name__)

default_global_attributes = {"instrument_manufacturer": "Star-Oddi", "source": None}

variables_attributes = {
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
}


def DAT(path, encoding="cp1252", kwargs_read_csv=None):
    def _standardize_attributes(item):
        item = re.sub(r"[\.\:]", "", item.strip().lower())
        return re.sub(r"\s", "_", item)

    if kwargs_read_csv is None:
        kwargs_read_csv = {}
    metadata = {}
    variables = {}
    original_header = ""
    with open(path, "r", encoding=encoding) as f:
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

        # Split metadata
        if metadata["date_&_time"] == "1":
            variables = {**{"time": {}}, **variables}

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
            **default_global_attributes,
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
            "start_time": pd.to_datetime(start_time).isoformat(),
            "end_time": pd.to_datetime(end_time).isoformat(),
            "date_created": pd.to_datetime(metadata.pop("created")).isoformat(),
            "original_file_header": original_header,
        }
        # Add variable attributes
        for var in ds:
            ds[var].attrs = {**variables[var], **variables_attributes.get(var, {})}
        return ds
