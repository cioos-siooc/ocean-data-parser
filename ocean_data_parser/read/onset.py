""" 
# Onset 
<https://www.onsetcomp.com/>

"""
import logging
import re
from csv import reader
from datetime import datetime

import numpy as np
import pandas as pd
import xarray
from dateutil.parser._parser import ParserError

from .utils import test_parsed_dataset

logger = logging.getLogger(__name__)
_onset_variables_mapping = {
    "#": "record_number",
    "Date Time": "time",
    "Temp": "temperature",
    "Intensity": "light_intensity",
    "Specific Conductance": "specific_conductance",
    "Low Range": "low_range",
    "EOF": "end_of_file",
    "End of File": "end_of_file",
    "Abs Pres Barom.": "barometric_pressure",
    "Pressure Barom.": "barometric_pressure",
    "Abs Pres": "pressure",
    "Sensor Depth": "sensor_depth",
    "Turbidity": "turbidity",
    "Water Level": "water_level",
}

_ignored_variables = [
    "record_number",
    "time",
    "button_up",
    "button_down",
    "host_connected",
    "end_of_file",
    "coupler_detached",
    "coupler_attached",
    "stopped",
    "started",
    "good_battery",
    "bad_battery",
    "host_connect",
    "batt",
    "low_power",
    "water_detect",
    "record",
    "",
]


def _parse_onset_time(time, timezone="UTC"):
    """Convert onset timestamps to pd.Timestamp objects"""
    if isinstance(time, np.datetime64):
        time_format = None
    elif re.match(r"\d\d\/\d\d\/\d\d\s+\d\d\:\d\d\:\d\d\s+\w\w", time):
        time_format = r"%m/%d/%y %I:%M:%S %p"
    elif re.match(r"\d\d\d\d\/\d\d\/\d\d\s+\d\d\:\d\d\:\d\d\s+\w\w", time):
        time_format = r"%Y/%m/%d %I:%M:%S %p"
    elif re.match(r"\d\d\/\d\d\/\d\d\s+\d\d\:\d\d", time):
        time_format = r"%m/%d/%y %H:%M"
    elif re.match(r"\d+\/\d+\/\d\d\s+\d\d\:\d\d", time):
        time_format = r"%m/%d/%y %H:%M"
    elif re.match(r"^\d\d\d\d\-\d\d\-\d\d\s+\d\d\:\d\d\:\d\d$", time):
        time_format = r"%Y-%m-%d %H:%M:%S"
    elif re.match(r"\d\d\d\d\-\d\d\-\d\d\s+\d\d\:\d\d\:\d\d (AM|PM)", time):
        time_format = r"%Y-%m-%d %I:%M:%S %p"
    elif re.match(r"^\d\d\-\d\d\-\d\d\s+\d{1,2}\:\d\d$", time):
        time_format = r"%y-%m-%d %H:%M"
    elif re.match(r"^\d\d\-\d\d\-\d\d\s+\d{1,2}\:\d\d\:\d\d$", time):
        time_format = r"%y-%m-%d %H:%M:%S"
    elif re.match(r"^\d\d\d\d\-\d\d\-\d\d\s+\d{1,2}\:\d\d$", time):
        time_format = r"%Y-%m-%d %H:%M"
    elif time in ("", None):
        return pd.NaT
    else:
        time_format = None
    try:
        return (
            pd.to_datetime(time, format=time_format)
            .tz_localize(timezone)
            .tz_convert("UTC")
        )
    except ParserError:
        logging.error("Failed to convert to timestamp: %s", time, exc_info=True)
        return pd.NaT


def _parse_onset_csv_header(header_lines):

    full_header = "\n".join(header_lines)
    header = {
        "instrument_manufacturer": "Onset",
        "history": "",
        "timezone": re.search(r"GMT\s*([\-\+\d\:]*)", full_header),
        "plot_title": re.search(r"Plot Title\: (\w*),+", full_header),
        "logger_sn": ",".join(set(re.findall(r"LGR S\/N\: (\d*)", full_header))),
        "sensor_sn": ",".join(set(re.findall(r"SEN S\/N\: (\d*)", full_header))),
        "instrument_sn": ",".join(
            set(
                re.findall(r"(?:SEN S\/N|LGR S\/N|Serial Number):\s*(\d+)", full_header)
            )
        ),
        "lbl": ",".join(set(re.findall(r"lbl: (\d*)", full_header))),
    }

    header = {
        key: value[1] if isinstance(value, re.Match) else value
        for key, value in header.items()
    }
    # Handle Columns
    original_columns = list(reader([header_lines[-1]], delimiter=",", quotechar='"'))[0]
    variables = {}
    for col in original_columns:
        # Ignore plot title from column names
        if header["plot_title"]:
            col = col.replace(header["plot_title"], "")

        column_with_units = re.sub(
            r"\s*\(*(LGR S\/N|SEN S\/N|LBL): .*",
            "",
            col,
        )
        column = re.split(r"\,|\(|\)", column_with_units)[0].strip()
        variables[column] = {
            "original_name": col,
            "units": re.split(r"\,|\(", column_with_units.replace(")", "").strip())[
                -1
            ].strip()
            if re.search(r"\,|\(", column_with_units)
            else None,
        }

    header["time_variables"] = [var for var in variables if "Date Time" in var]

    if header["timezone"] is None:
        logger.warning("No Timezone available within this file. UTC will be assumed.")
        header["timezone"] = "UTC"

    return header, variables


def _standardized_variable_mapping(variables):
    """Standardize onset variable names"""
    return {
        var: _onset_variables_mapping[var]
        if var in _onset_variables_mapping
        else var.lower().replace(" ", "_")
        for var in variables
    }


def csv(
    path: str,
    convert_units_to_si: bool = True,
    read_csv_kwargs: dict = None,
    standardize_variable_names: bool = True,
) -> xarray.Dataset:

    """Parses the Onset CSV format generate by HOBOware into a xarray object
    Inputs:
        path: The path to the CSV file
        convert_units_to_si: Whether to standardize data units to SI units
        read_csv_kwargs: dictionary of keyword arguments to be passed to pd.read_csv
        standardize_variable_names: Rename the variable names a standardize name convention
    Returns:
        xarray.Dataset
    """
    if read_csv_kwargs is None:
        read_csv_kwargs = {}
    raw_header = []
    with open(
        path,
        encoding=read_csv_kwargs.get("encoding", "UTF-8"),
        errors=read_csv_kwargs.get("encoding_errors"),
    ) as f:
        raw_header += [f.readline().replace("\n", "")]
        header_lines = 1
        if "Serial Number:" in raw_header[0]:
            # skip second empty line
            header_lines += 1
            f.readline()  #
        # Read csv columns
        raw_header += [f.readline()]

    # Parse onset header
    header, variables = _parse_onset_csv_header(raw_header)

    # Inputs to pd.read_csv
    column_names = [var for var in list(variables.keys()) if var]
    df = pd.read_csv(
        path,
        na_values=[" "],
        infer_datetime_format=True,
        parse_dates=header["time_variables"],
        converters={
            header["time_variables"][0]: lambda col: _parse_onset_time(
                col, header["timezone"]
            )
        },
        sep=",",
        header=header_lines,
        memory_map=True,
        names=column_names,
        usecols=[id for id, name in enumerate(column_names)],
        **read_csv_kwargs,
    )

    # Convert to dataset
    ds = df.to_xarray()
    ds.attrs = header
    for var in ds:
        ds[var].attrs = variables[var]

    if standardize_variable_names:
        ds = ds.rename_vars(_standardized_variable_mapping(ds))
        # Detect instrument type based on variables available
        ds.attrs["instrument_type"] = _detect_instrument_type(ds)

    # # Review units and convert SI system
    if convert_units_to_si:
        if standardize_variable_names:
            if "temperature" in ds and ("C" not in ds["temperature"].attrs["units"]):
                temp_units = ds["temperature"].attrs["units"]
                string_comment = f"Convert temperature ({temp_units}) to degree Celsius [(degF-32)/1.8000]"
                logger.warning(string_comment)
                ds["temperature"] = (ds["temperature"] - 32.0) / 1.8000
                ds["temperature"].attrs["units"] = "degC"
                ds.attrs["history"] += f"{datetime.now()} {string_comment}"
            if (
                "conductivity" in ds
                and "uS/cm" not in ds["conductivity"].attrs["units"]
            ):
                logger.warning(
                    "Unknown conductivity units (%s)", ds["conductivity"].attrs["units"]
                )
        else:
            logger.warning(
                "Unit conversion is not supported if standardize_variable_names=False"
            )

    # Test daylight saving issue
    dt = ds["time"].diff("index")
    sampling_interval = dt.median().values
    dst_fall = -pd.Timedelta("1h") + sampling_interval
    dst_spring = pd.Timedelta("1h") + sampling_interval
    if any(dt == dst_fall):
        logger.warning(
            "Time gaps (=%s) for sampling interval of %s suggest a Fall daylight saving issue is present",
            dst_fall,
            sampling_interval,
        )
    if any(dt == dst_spring):
        logger.warning(
            "Time gaps (=%s) for sampling interval of %s suggest a Spring daylight saving issue is present",
            dst_fall,
            sampling_interval,
        )
    # Test dataset
    test_parsed_dataset(ds)
    return ds


def _detect_instrument_type(ds):
    """Detect instrument type based on variables available in the dataset."""
    # Try to match instrument type based on variables available (this information is
    # unfortnately not available withint the CSV)
    vars_of_interest = {
        var
        for var in ds
        if var not in _ignored_variables and not var.startswith("unnamed")
    }

    if vars_of_interest == {"temperature", "light_intensity"}:
        instrument_type = "Pendant"
    elif vars_of_interest == {"specific_conductance", "temperature", "low_range"}:
        instrument_type = "CT"
    elif vars_of_interest == {"temperature", "specific_conductance"}:
        instrument_type = "CT"
    elif vars_of_interest == {"temperature"}:
        instrument_type = "Tidbit"
    elif vars_of_interest == {"temperature", "sensor_depth"}:
        instrument_type = "PT"
    elif vars_of_interest == {"temperature", "pressure", "sensor_depth"}:
        instrument_type = "PT"
    elif vars_of_interest == {
        "temperature",
        "barometric_pressure",
        "pressure",
        "sensor_depth",
    }:
        instrument_type = "WL"
    elif vars_of_interest == {
        "temperature",
        "barometric_pressure",
        "pressure",
        "water_level",
    }:
        instrument_type = "WL"
    elif vars_of_interest == {"temperature", "pressure"}:
        instrument_type = "airPT"
    elif vars_of_interest == {"barometric_pressure"}:
        instrument_type = "airP"
    elif vars_of_interest == {"turbidity"}:
        instrument_type = "turbidity"
    else:
        instrument_type = "unknown"
        logger.warning(
            "Unknown Hobo instrument type with variables: %s", vars_of_interest
        )
    return instrument_type
