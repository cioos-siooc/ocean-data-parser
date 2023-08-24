"""
# PME Instruments 
<https://www.pme.com/>

"""

import logging
import re
import warnings
from datetime import datetime
from typing import Union

import pandas as pd
import xarray as xr
from o2conversion import O2ctoO2p, O2ctoO2s

from ocean_data_parser.parsers.utils import standardize_dataset

logger = logging.getLogger(__name__)
variable_attributes = {
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

vars_rename = {
    "Time (sec)": "time",
    "T (deg C)": "temperature",
    "BV (Volts)": "batt_volt",
    "DO (mg/l)": "do_mg_l",
    "Q ()": "q",
}

variable_attributes = {
    "Time (sec)": dict(long_name="Time", standard_name="time"),
    "T (deg C)": dict(
        long_name="Temperature",
        units="degrees_celsius",
        standard_name="sea_water_temperature",
    ),
    "BV (Volts)": dict(long_name="Battery Voltage", units="Volts"),
    "DO (mg/l)": dict(
        long_name="Dissolved Oxygen Concentration",
        units="mg/l",
        standard_name="mass_concentration_of_oxygen_in_sea_water",
    ),
    "Q ()": dict(long_name="Q"),
}

global_attributes = {"Conventions": "CF-1.6"}


def minidot_txt(
    path: str, read_csv_kwargs: dict = None, rename_variables: bool = True
) -> xr.Dataset:
    """
    minidot_txt parses the txt format provided by the PME Minidot instruments.
    """

    def _append_to_history(msg):
        ds.attrs["history"] += f"{pd.Timestamp.utcnow():%Y-%m-%dT%H:%M:%SZ} {msg}"

    # Default read_csv_kwargs
    if read_csv_kwargs is None:
        read_csv_kwargs = {}

    # Read MiniDot
    with open(
        path,
        "r",
        encoding=read_csv_kwargs.get("encoding", "utf-8"),
        errors=read_csv_kwargs.get("encoding_errors"),
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

        # Read the data with pandas
        ds = pd.read_csv(
            f,
            converters={0: lambda x: pd.to_datetime(x, unit="s", utc=True)},
            **read_csv_kwargs,
        ).to_xarray()

    # Strip whitespaces from variables names
    ds = ds.rename({var: var.strip() for var in ds.keys()})

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
        if var not in variable_attributes:
            logger.warning("Unknown variable: %s", var)
            continue
        ds[var].attrs = variable_attributes[var]

    if rename_variables:
        ds = ds.rename_vars(vars_rename)
    ds.attrs[
        "history"
    ] += f"\n{datetime.now().isoformat()} Rename variables: {vars_rename}"

    ds = standardize_dataset(ds)
    return ds


def minidot_txts(paths: Union[list, str]) -> xr.Dataset:
    """Parse PME Minidots txt files

    Args:
        paths (listorstr): List of file paths to read.

    Returns:
        xr.Dataset: xarray dataset which is compliant with CF-1.6
    """
    # If a single string is givien, assume only one path
    if type(paths) is str:
        paths = [paths]

    datasets = []
    for path in paths:
        # Ignore concatenated Cat.TXT files or not TXT file
        if path.endswith("Cat.TXT") or not path.endswith(("TXT", "txt")):
            print(f"Ignore {path}")
            continue
        # Read txt file
        datasets += minidot_txt(path)

    return xr.merge(datasets)


def minidot_cat(path: str, read_csv_kwargs: dict = None) -> xr.Dataset:
    """
    cat reads PME MiniDot concatenated CAT files
    """
    if read_csv_kwargs is None:
        read_csv_kwargs = {}
    with open(path, "r", encoding=read_csv_kwargs.get("encoding", "utf8")) as f:
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

        ds = pd.read_csv(f, names=names, **read_csv_kwargs).to_xarray()

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
