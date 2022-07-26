"""
PME Instruments https://www.pme.com/
"""

import logging
import re
import warnings
from datetime import datetime

import pandas as pd
import xarray as xr

from ..convert.oxygen import O2ctoO2p, O2ctoO2s

logger = logging.getLogger(__name__)
vars_attributes = {
    "T (deg C)": {
        "units": "degree_c",
        "standard_name": "temperature_of_sensor_for_oxygen_in_sea_water",
    },
    "DO (mg/l)": {
        "units": "Volts",
        "standard_name": "mass_concentration_of_oxygen_in_sea_water",
        "comments": "at Salinity=0 and pressure=0",
    },
    "BV (Volts)": {"long_name": "Battery Voltage", "units": "Volts"},
}

vars_rename = {
    "Time (sec)": "time",
    "T (deg C)": "temperature",
    "BV (Volts)": "batt_volt",
    "DO (mg/l)": "do_mg_l",
    "Q ()": "q",
}


def minidot_txt(path, read_csv_kwargs=None):
    """
    minidot_txt parses the txt format provided by the PME Minidot instruments.
    """
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
        metadata = re.search(
            r"OS REV: (?P<software_version>\d+\.\d+) Sensor Cal: (?P<instrument_calibration>\d*)",
            f.readline(),
        )

        # If metadata is null than it's likely not a minidot file
        if metadata is None:
            warnings.warn("Failed to read: {path}", RuntimeWarning)
            return pd.DataFrame(), None

        # Read the data with pandas
        ds = pd.read_csv(
            f,
            parse_dates=[0],
            infer_datetime_format=True,
            date_parser=lambda x: pd.to_datetime(x, unit="s", utc=True),
            **read_csv_kwargs,
        ).to_xarray()

    # Strip whitespaces from variables names
    ds = ds.rename({var: var.strip() for var in ds.keys()})

    # Global attributes
    ds.attrs = metadata.groupdict()
    ds.attrs.update(
        {
            "instrument_manufacturer": "PME",
            "instrument_model": "MiniDot",
            "instrument_sn": serial_number,
            "history": "",
        }
    )

    # Retrieve raw saturation values from minidot, assume 0 salinity and at surface (pressure=0).
    if "DO (mg/l)" in ds:
        ds["do_perc_sat"] = retrieve_oxygen_saturation_percent(
            ds["DO (mg/l)"], ds["T (deg C)"], salinity=0, pressure=0, units="mg/l"
        )
        ds.attrs[
            "history"
        ] += f"\n{datetime.now().isoformat()} Retrieve Oxygen Saturation Percentage values from 'DO (mg/l)' and 'T (deg C)' by assuming 0 salinity and at surface (pressure=0)"
        ds["po2"] = retrieve_oxygen_saturation_percent(
            ds["DO (mg/l)"], ds["T (deg C)"], salinity=0, pressure=0, units="mg/l"
        )
        ds.attrs[
            "history"
        ] += f"\n{datetime.now().isoformat()} Retrieve Partial Pressure of Oxygen values from 'DO (mg/l)' and 'T (deg C)' by assuming 0 salinity and at surface (pressure=0)"

    # Add attributes to the dataset and rename variables to mapped names.
    for var in ds:
        if var in vars_attributes:
            ds[var].attrs = vars_attributes[var]
    ds = ds.rename_vars(vars_rename)
    ds.attrs[
        "history"
    ] += f"\n{datetime.now().isoformat()} Rename variables: {vars_rename}"
    return ds


def minidot_txts(paths: list or str):
    """
    txts reads individual minidot txt files,
    add the calibration, serial_number and software version
    information as a new column and return a dataframe.
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


def minidot_cat(path, read_csv_kwargs=None):
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


def retrieve_oxygen_saturation_percent(
    do_conc,
    temp,
    pressure=0,
    salinity=0,
    units="mg/l",
):
    """Convert minidot raw oxygen concentration corrected for temperature and add fix salinity and pressure to saturation percent."""
    # Convert mg/l to umol/l concentration
    if units == "mg/l":
        do_conc = 31.2512 * do_conc
        units = "umol/l"
    if units != "umol/l":
        logger.error("Uncompatble units: %s", units)
        return

    return O2ctoO2s(do_conc, temp, salinity, pressure)


def retrieve_po2(
    do_conc,
    temp,
    pressure=0,
    salinity=0,
    units="mg/l",
):
    """Convert minidot raw oxygen concentration corrected for temperature and add fix salinity and pressure to saturation percent."""
    # Convert mg/l to umol/l concentration
    if units == "mg/l":
        do_conc = 31.2512 * do_conc
        units = "umol/l"
    if units != "umol/l":
        logger.error("Uncompatble units: %s", units)
        return

    return O2ctoO2p(do_conc, temp, salinity, pressure)
