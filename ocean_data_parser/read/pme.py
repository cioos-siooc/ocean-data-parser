"""
PME Instruments https://www.pme.com/
"""

import logging
import re
import warnings
from datetime import datetime

import pandas as pd

from ..convert.oxygen import O2ctoO2s

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


def minidot_txt(path, output="xarray"):
    """
    minidot_txt parses the txt format provided by the PME Minidot instruments.
    """
    # Read MiniDot
    with open(path, "r") as f:
        # Read the headre
        serial_number = f.readline().replace("\n", "")
        metadata = re.search(
            "OS REV: (?P<software_version>\d+\.\d+) Sensor Cal: (?P<instrument_calibration>\d*)",
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

    # Add attributes to the dataset and rename variables to mapped names.
    for var in ds:
        if var in vars_attributes:
            ds[var].attrs = vars_attributes[var]
    ds = ds.rename_vars(vars_rename)
    ds.attrs[
        "history"
    ] += f"\n{datetime.now().isoformat()} Apply variable rename: {vars_rename}"

    # Output
    if output == "xarray":
        return ds
    elif output == "dataframe":
        df = ds.to_dataframe()
        add_attributes = [
            "instrument_sn",
            "instrument_model",
            "instrument_manufacturer",
        ]
        for att in add_attributes:
            df.insert(0, att, ds.attrs[att])
        return df


def minidot_txts(paths: list or str):
    """
    txts reads individual minidot txt files,
    add the calibration, serial_number and software version
    information as a new column and return a dataframe.
    """
    # If a single string is givien, assume only one path
    if type(paths) is str:
        paths = [paths]

    df = pd.DataFrame()
    for path in paths:
        # Ignore concatenated Cat.TXT files or not TXT file
        if path.endswith("Cat.TXT") or not path.endswith(("TXT", "txt")):
            print(f"Ignore {path}")
            continue
        # Read txt file
        df = df.append(minidot_txt(path, output="dataframe"))

    return df


def minidot_cat(path):
    """
    cat reads PME MiniDot concatenated CAT files
    """

    with open(path, "r") as f:
        header = f.readline()

        if header != "MiniDOT Logger Concatenated Data File\n":
            raise RuntimeError(
                "Can't recognize the CAT file! \nCAT File should start with ''MiniDOT Logger Concatenated Data File'"
            )
        # Read header and column names and units
        header = [f.readline() for _ in range(6)]
        columns = [f.readline() for _ in range(2)]

        names = columns[0].replace("\n", "").split(",")
        units = columns[1].replace("\n", "")

        ds = pd.read_csv(f, names=names).to_xarray()

    # Include units
    for name, units in zip(names, units):
        if units:
            ds[name].attrs[units] = units

    # Extract metadata from header
    ds.attrs = re.search(
        (
            "Sensor:\s*(?P<instrument_sn>.*)\n"
            + "Concatenation Date:\s*(?P<concatenation_date>.*)\n\n"
            + "DO concentration compensated for salinity:\s*(?P<reference_salinity>.*)\n"
            + "Saturation computed at elevation:\s*(?P<elevation>.*)\n"
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
        logger.error(f"Uncompatble units: {units}")
        return

    return O2ctoO2s(do_conc, temp, salinity, pressure)
