"""
# Van Essen Instruments
<https://www.vanessen.com/>

"""
import json
import logging
import re

import pandas as pd
import xarray

from ocean_data_parser.parsers.utils import standardize_dataset

logger = logging.getLogger(__name__)

global_attributes = {"Convention": "CF-1.6"}

van_essen_variable_mapping = {
    "PRESSURE": "pressure",
    "TEMPERATURE": "temperature",
    "CONDUCTIVITY": "conductivity",
    "SPEC.COND.": "specific_conductance",
}

van_essen_vocabulary = {
    "PRESSURE": {"standard_name": "sea_water_pressure"},
    "TEMPERATURE": {"standard_name": "sea_water_temperature"},
    "CONDUCTIVITY": {
        "standard_name": "sea_water_electrical_conductivity",
    },
}


def mon(
    file_path: str,
    standardize_variable_names: bool = True,
    convert_pressure_to_dbar: bool = True,
) -> xarray.Dataset:
    """Parse Van Essen Instruments mon format to NetCDF.

    Args:
        file_path (str): File path to load
        standardize_variable_names (bool, optional): Rename variables. Defaults to True.
        convert_pressure_to_dbar (bool, optional): Convert pressure data in
            cmH2O/mH2O to dbar. Defaults to True.

    Returns:
        xarray.Dataset: Parsed dataset
    """
    with open(
        file_path,
    ) as fid:
        line = ""
        section = "header_info"
        info = {section: {}}
        while not line.startswith("[Data]\n"):
            # Read line by line
            line = fid.readline()
            if re.match(r"\[.+\]", line):
                section = re.search(r"\[(.+)\]", line)[1]
                if section not in info:
                    info[section] = {}
            elif re.match(r"\s*(?P<key>[\w\s]+)(\=|\:)(?P<value>.+)", line):
                item = re.search(r"\s*(?P<key>[\w\s]+)(\=|\:)(?P<value>.+)", line)
                info[section][item["key"].strip()] = item["value"].strip()
            else:
                continue

        # Regroup channels
        info["Channel"] = {}
        for key, items in info.items():
            channel_number = re.search(r"Channel (\d+) from data header", key)
            if channel_number:
                info["Channel"][items["Identification"]] = items
                info["Channel"][items["Identification"]]["id"] = int(channel_number[1])

        # Define column names
        channel_names = ["time"] + [
            attrs["Identification"] for id, attrs in info["Channel"].items()
        ]
        # Read the rest with pandas
        # Find first how many records exist
        info["n_records"] = int(fid.readline())

        # Retrieve timezone
        timezone = (
            re.search(r"UTC([\-\+]*\d+)", info["Series settings"]["Instrument number"])[
                1
            ]
            + ":00"
        )

        # Read data (Seperator is minimum 2 spaces)
        df = pd.read_csv(
            fid,
            names=channel_names,
            header=None,
            sep=r"\s\s+",
            skipfooter=1,
            engine="python",
            comment="END OF DATA FILE OF DATALOGGER FOR WINDOWS",
        )

    # handle time variable
    df["time"] = pd.to_datetime(df["time"] + timezone, utc=True)

    # If there's less data then expected send a warning
    if len(df) < info["n_records"]:
        assert RuntimeWarning(
            f'Missing data, expected {info["n_records"]} and found only {len(df)}'
        )
    # Convert to xarray
    ds = df.to_xarray()

    # Generate global_attributes
    ds.attrs = {
        **global_attributes,
        "instrument_manufacturer": "Van Essen Instruments",
        "instrument_type": info["Logger settings"]["Instrument type"],
        "instrument_sn": info["Logger settings"]["Serial number"],
        "time_coverage_resolution": info["Logger settings"]["Sample period"],
        "original_metadata": json.dumps(info),
    }

    # Define variable attributes
    for var, attrs in info["Channel"].items():
        ds[var].attrs = {
            attr.lower().replace(" ", "_"): value
            for attr, value in attrs.items()
            if attr not in ["Identification", "id"]
        }
        reference_units = attrs["Reference level"].rsplit(" ", 1)[1]
        range_units = attrs["Reference level"].rsplit(" ", 1)[1]
        if range_units == reference_units:
            ds[var].attrs["units"] = reference_units
        else:
            logger.error("Failed to retrieve %s units from attributes %s", var, attrs)

    # Drop column number in variable names
    ds = ds.rename({var: re.sub(r"^\d+\:\s*", "", var) for var in ds})

    # Add H2O to PRESSURE units [m, cm]
    if "PRESSURE" in ds:
        if ds["PRESSURE"].attrs["units"] in ["m", "cm"]:
            ds["PRESSURE"].attrs["units"] += "H2O"

        # IF PRESSURE in cm, convert to meter
        if ds["PRESSURE"].attrs["units"] == "cmH2O":
            logger.warning("Convert Pressure from cm to m")
            ds["PRESSURE"] = ds["PRESSURE"] / 100
            ds["PRESSURE"].attrs["units"] = "mH2O"

        if convert_pressure_to_dbar:
            ds["PRESSURE"] = ds["PRESSURE"] * 0.980665
            ds["PRESSURE"].attrs["units"] = "dbar"

    # Add Conductivity if missing
    if "CONDUCTIVITY" not in ds and "SPEC.COND." in ds:
        ds["CONDUCTIVITY"] = _specific_conductivity_to_conductivity(
            ds["SPEC.COND."], ds["TEMPERATURE"]
        )

    # Specific Conductance if missing
    if "CONDUCTIVITY" in ds and "SPEC.COND." not in ds:
        ds["SPEC.COND."] = _conductivity_to_specific_conductivity(
            ds["CONDUCTIVITY"], ds["TEMPERATURE"]
        )

    # Add vocabulary
    for var, attrs in van_essen_vocabulary.items():
        ds[var].attrs.update(attrs)

    # Standardize variables names
    if standardize_variable_names:
        ds = ds.rename(van_essen_variable_mapping)

    return standardize_dataset(ds)


def _specific_conductivity_to_conductivity(
    spec_cond, temp, theta=1.91 / 100, temp_ref=25
):
    """Apply specific_conductivity conversion to conductivity based on the manufacturer equation."""
    return (100 + theta * (temp - temp_ref)) / 100 * spec_cond


def _conductivity_to_specific_conductivity(cond, temp, theta=1.91 / 100, temp_ref=25):
    """Apply conductivity conversion to specific_conductivity based on the manufacturer equation."""
    return 100 / (100 + theta * (temp - temp_ref)) * cond
