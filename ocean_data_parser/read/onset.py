import pandas as pd
import numpy as np
import re
from datetime import datetime
from csv import reader
import logging
from .utils import test_parsed_dataset

from dateutil.parser._parser import ParserError

logger = logging.getLogger(__name__)
onset_variables_mapping = {
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

ignored_variables = [
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


def parse_onset_time(time, timezone="UTC"):
    if type(time) is np.datetime64:
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
        logging.error(f"Failed to convert to timestamp: {time}", exc_info=True)
        return pd.NaT


def parse_onset_csv_header(header_lines):

    full_header = "\n".join(header_lines)
    header = {
        "instrument_manufacturer": "Onset",
        "history": "",
        "timezone": re.search("GMT\s*([\-\+\d\:]*)", full_header),
        "plot_title": re.search("Plot Title\: (\w*)\,+", full_header),
        "logger_sn": ",".join(set(re.findall("LGR S\/N\: (\d*)", full_header))),
        "sensor_sn": ",".join(set(re.findall("SEN S\/N\: (\d*)", full_header))),
        "instrument_sn": ",".join(
            set(re.findall("(?:SEN S\/N|LGR S\/N|Serial Number):\s*(\d+)", full_header))
        ),
        "lbl": ",".join(set(re.findall("lbl: (\d*)", full_header))),
    }

    # Handle Columns
    original_columns = list(reader([header_lines[-1]], delimiter=",", quotechar='"'))[0]
    variables = {}
    for col in original_columns:
        column_with_units = re.sub(
            f"\s*\(*(LGR|SEN) S\/N\: .*|[^\(]*{header['plot_title']}|\(LBL\:\w*",
            "",
            col,
        )
        column = re.split("\,|\(|\)", column_with_units)[0].strip()
        variables[column] = {
            "original_name": col,
            "units": re.split("\,|\(", column_with_units.replace(")", "").strip())[
                -1
            ].strip()
            if re.search("\,|\(", column_with_units)
            else None,
        }

    header["time_variables"] = [var for var in variables if "Date Time" in var]

    if header["timezone"] is None:
        logger.warning("No Timezone available within this file. UTC will be assumed.")
        header["timezone"] = "UTC"

    header = {
        key: value[1] if type(value) is re.Match else value
        for key, value in header.items()
    }

    return header, variables


def standardized_variable_mapping(vars):
    return {
        var: onset_variables_mapping[var]
        if var in onset_variables_mapping
        else var.lower().replace(" ", "_")
        for var in vars
    }


def csv(
    path,
    output: str = "xarray",
    convert_units_to_si: bool = True,
    input_read_csv_kwargs: dict = None,
    standardize_variable_names: bool = True,
):

    """tidbit_csv parses the Onset Tidbit CSV format into a pandas dataframe

    Returns:
        df: data in pandas dataframe
        metadata: metadata dictionary
    """
    if input_read_csv_kwargs is None:
        input_read_csv_kwargs = {}
    encoding = input_read_csv_kwargs.get("encoding", "UTF-8")
    encoding_errors = input_read_csv_kwargs.get("encoding_errors")
    raw_header = []
    with open(path, "r", encoding=encoding, errors=encoding_errors) as f:
        raw_header += [f.readline().replace("\n", "")]
        header_lines = 1
        if "Serial Number:" in raw_header[0]:
            # skip second empty line
            header_lines += 1
            f.readline()  #
        # Read csv columns
        raw_header += [f.readline()]

    # Parse onset header
    header, variables = parse_onset_csv_header(raw_header)

    # Inputs to pd.read_csv
    column_names = [var for var in list(variables.keys()) if var]
    read_csv_kwargs = {
        "na_values": [" "],
        "infer_datetime_format": True,
        "parse_dates": header["time_variables"],
        "converters": {
            header["time_variables"][0]: lambda col: parse_onset_time(
                col, header["timezone"]
            )
        },
        "sep": ",",
        "header": header_lines,
        "memory_map": True,
        "encoding": encoding,
        "names": column_names,
        "usecols": [id for id, name in enumerate(column_names)],
    }
    read_csv_kwargs.update(input_read_csv_kwargs)
    df = pd.read_csv(path, **read_csv_kwargs)

    # Convert to dataset
    ds = df.to_xarray()
    ds.attrs = header
    for var in ds:
        ds[var].attrs = variables[var]

    if standardize_variable_names:
        ds = ds.rename_vars(standardized_variable_mapping(ds))
        # Detect instrument type based on variables available
        ds.attrs["instrument_type"] = detect_instrument_type(ds)

    # # Review units and convert SI system
    if convert_units_to_si and standardize_variable_names:
        if "temperature" in ds and ("C" not in ds["temperature"].attrs["units"]):
            string_comment = f"Convert temperature ({ds['temperature'].attrs['units']}) to degree Celsius [(degF-32)/1.8000]"
            logger.warning(string_comment)
            ds["temperature"] = (ds["temperature"] - 32.0) / 1.8000
            ds["temperature"].attrs["units"] = "degC"
            ds.attrs["history"] += f"{datetime.now()} {string_comment}"
        if "conductivity" in ds and "uS/cm" not in ds["conductivity"].attrs["units"]:
            logger.warning(
                f"Unknown conductivity units ({ds['conductivity'].attrs['units']})"
            )
    elif convert_units_to_si:
        logger.warning(
            "Unit conversion is not supported if standardize_variable_names=False"
        )

    # Test dataset
    test_parsed_dataset(ds)

    if output == "xarray":
        return ds
    df = ds.to_dataframe()
    # Include instrument information within the dataframe
    for var in ["instrument_manufacturer", "instrument_type", "instrument_sn"][::-1]:
        df.insert(0, var, ds.attrs[var])
    return df


def detect_instrument_type(ds):
    """Detect instrument type based on variables available in the dataset."""
    # Try to match instrument type based on variables available (this information is unfortnately not available withint the CSV)
    vars_of_interest = {
        var
        for var in ds
        if var not in ignored_variables and not var.startswith("unnamed")
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
            f"Unknown Hobo instrument type with variables: {vars_of_interest}"
        )
    return instrument_type
