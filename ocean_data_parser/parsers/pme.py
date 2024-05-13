"""
[Precision Measurement Engineering (PME)](https://www.pme.com/)
is a company that manufactures instruments to measure different water properties.
"""

import logging
import re
import warnings
from typing import Union

import pandas as pd
import xarray as xr
from o2conversion import O2ctoO2p, O2ctoO2s

from ocean_data_parser.parsers.utils import standardize_dataset

logger = logging.getLogger(__name__)
VARIABLE_ATTRIBUTES = {
    "index": {},
    "Time (sec)": {"long_name": "Time", "standard_name": "time"},
    "T (deg C)": {
        "long_name": "Time",
        "units": "degrees_celsius",
        "standard_name": "temperature_of_sensor_for_oxygen_in_sea_water",
    },
    "DO (mg/l)": {
        "long_name": "Dissolved Oxygen Conentration",
        "units": "mg/L",
        "standard_name": "mass_concentration_of_oxygen_in_sea_water",
        "comments": "at Salinity=0 and pressure=0",
    },
    "DO (perc)": {
        "long_name": "Dissolved Oxygen Percentage of saturation",
        "units": "percent",
        "standard_name": "fractional_saturation_of_oxygen_in_sea_water",
        "comments": "at Salinity=0 and pressure=0",
    },
    "pO2 (mbar)": {
        "long_name": "Partial Pressure of Oxygen",
        "units": "mbar",
        "comments": "at Salinity=0 and pressure=0",
    },
    "BV (Volts)": {"long_name": "Battery Voltage", "units": "Volts"},
    "Q ()": {"long_name": "Q"},
}

VARIABLE_RENAMING_MAPPING = {
    "Time (sec)": "time",
    "T (deg C)": "temperature",
    "BV (Volts)": "batt_volt",
    "DO (mg/l)": "do_mg_l",
    "DO (perc)": "do_perc",
    "pO2 (mbar)": "po2_mbar",
    "Q ()": "q",
}

global_attributes = {"Conventions": "CF-1.6"}


def minidot_txt(
    path: str,
    rename_variables: bool = True,
    encoding: str = "utf-8",
    errors: str = "strict",
    timezone: str = "UTC",
) -> xr.Dataset:
    """Parse PME MiniDot txt file

    Args:
        path (str): txt file path to read
        rename_variables (bool, optional): _description_. Defaults to True.
        encoding (str, optional): File encoding. Defaults to 'utf-8'.
        errors (str, optional): Error handling. Defaults to 'strict'.

    Returns:
        xarray.Dataset
    """

    def _append_to_history(msg):
        ds.attrs["history"] += f"{pd.Timestamp.utcnow():%Y-%m-%dT%H:%M:%SZ} {msg}"

    # Read MiniDot
    with open(
        path,
        "r",
        encoding=encoding,
        errors=errors,
    ) as f:
        # Read the headre
        serial_number = f.readline().replace("\n", "")
        logger.debug("Parse file from serial number: %s", serial_number)
        metadata = re.search(
            (
                r"OS REV: (?P<software_version>\d+\.\d+)\s"
                r"Sensor Cal: (?P<instrument_calibration>\d*)"
            ),
            f.readline(),
        )

        # If metadata is null than it's likely not a minidot file
        if metadata is None:
            warnings.warn("Failed to read: {path}", RuntimeWarning)
            return pd.DataFrame(), None
        
        # Parse column names
        columns = [item.strip() for item in f.readline().split(',')]

        # Read the data with pandas
        df = pd.read_csv(
            f,
            converters={0: lambda x: pd.Timestamp(int(x), unit="s")},
            encoding=encoding,
            encoding_errors=errors,
            names=columns,
            header = None,
        )
        ds = df.to_xarray()

    ds["Time (sec)"] = ds['Time (sec)'].to_index().tz_localize(timezone)
    # Global attributes
    ds.attrs = {
        **global_attributes,
        **metadata.groupdict(),
        "instrument_manufacturer": "PME",
        "instrument_model": "MiniDot",
        "instrument_sn": serial_number,
        "history": "",
    }

    # Retrieve raw saturation values from minidot
    #  assume:
    #   - 0 salinity
    #   - surface (pressure=0).
    if "DO (mg/l)" in ds:
        ds["DO (perc)"] = O2ctoO2s(31.2512 * ds["DO (mg/l)"], ds["T (deg C)"], S=0, P=0)
        _append_to_history(
            "Derive DO (perc) from = "
            "o2Conversion.O2ctoO2s( 31.2512*'DO (mg/l)', 'T (deg C)', S=0, P=0)",
        )

        ds["pO2 (mbar)"] = O2ctoO2p(
            31.2512 * ds["DO (mg/l)"], ds["T (deg C)"], S=0, P=0
        )
        _append_to_history(
            "Derive pO2 (mbar) from = "
            "o2Conversion.O2ctoO2s(31.2512*'DO (mg/l)', 'T (deg C)', S=0, P=0)",
        )

    # Add attributes to the dataset and rename variables to mapped names.
    for var in ds.variables:
        if var not in VARIABLE_ATTRIBUTES:
            logger.warning("Unknown variable: %s", var)
            continue
        ds[var].attrs = VARIABLE_ATTRIBUTES[var]

    if rename_variables:
        ds = ds.rename_vars(VARIABLE_RENAMING_MAPPING)
    ds.attrs[
        "history"
    ] += f"\n{pd.Timestamp.now().isoformat()} Rename variables: {VARIABLE_RENAMING_MAPPING}"

    ds = standardize_dataset(ds)
    return ds


def minidot_txts(
    paths: Union[list, str], encoding: str = "utf-8", errors: str = "strict"
) -> xr.Dataset:
    """Parse PME Minidots txt files

    Args:
        paths (listorstr): List of file paths to read.
        encoding (str, optional): File encoding. Defaults to 'utf-8'.
        errors (str, optional): Error handling. Defaults to 'strict'.

    Returns:
        xr.Dataset: xarray dataset which is compliant with CF-1.6
    """
    # If a single string is givien, assume only one path
    if isinstance(paths, str):
        paths = [paths]

    datasets = []
    for path in paths:
        # Ignore concatenated Cat.TXT files or not TXT file
        if path.endswith("Cat.TXT") or not path.endswith(("TXT", "txt")):
            print(f"Ignore {path}")
            continue
        # Read txt file
        datasets += minidot_txt(path, encoding=encoding, errors=errors)

    return xr.merge(datasets)


def minidot_cat(
    path: str, encoding: str = "utf-8", errors: str = "strict"
) -> xr.Dataset:
    """cat reads PME MiniDot concatenated CAT files

    Args:
        path (str): File path to read
        encoding (str, optional): File encoding. Defaults to 'utf-8'.
        errors (str, optional): Error handling. Defaults to 'strict'.

    Returns:
        xr.Dataset: xarray dataset which is compliant with CF-1.6
    """
    with open(path, "r", encoding=encoding, errors=errors) as f:
        header = f.readline()

        if header != "MiniDOT Logger Concatenated Data File\n":
            raise RuntimeError(
                "Can't recognize the CAT file! \nCAT File should start with ''MiniDOT Logger Concatenated Data File'"
            )
        # Read header and column names and units
        header = [f.readline() for _ in range(6)]
        columns = [f.readline() for _ in range(2)]

        names = columns[0].replace(r"\n", "").split(",")
        units = columns[1].replace(r"\n", "")

        ds = pd.read_csv(
            f, names=names, encoding=encoding, encoding_errors=errors
        ).to_xarray()

    # Include units
    for name, units in zip(names, units):
        if units:
            ds[name].attrs[units] = units

    # Extract metadata from header
    ds.attrs = re.search(
        (
            r"Sensor:\s*(?P<instrument_sn>.*)\n"
            + r"Concatenation Date:\s*(?P<concatenation_date>.*)\n\n"
            + r"DO concentration compensated for salinity:\s*(?P<reference_salinity>.*)\n"
            + r"Saturation computed at elevation:\s*(?P<elevation>.*)\n"
        ),
        "".join(header),
    ).groupdict()

    return ds
