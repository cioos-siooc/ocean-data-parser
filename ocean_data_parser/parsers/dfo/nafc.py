"""
P-files is a file format used by the DFO NewfoundLand office.

According to the pfile documentation, the format is:

1) NAFC_Y2K_HEADER
2) 3 single line 80 byte headers, the formats of which is described on an attached page.
3) A variable length block of processing history information
4) A line of channel name identifiers
5) A start of data flag line -- DATA --

"""

import logging
import re
from pathlib import Path
from typing import Union

import gsw
import numpy as np
import pandas as pd
import xarray as xr

from ocean_data_parser.parsers.utils import standardize_dataset
from ocean_data_parser.vocabularies.load import (
    dfo_nafc_p_file_vocabulary,
    dfo_platforms,
)

logger = logging.getLogger(__name__)
MODULE_PATH = Path(__file__).parent
p_file_vocabulary = dfo_nafc_p_file_vocabulary()
p_file_shipcode = dfo_platforms()
# nafc_instruments = pd.read_csv(
#     MODULE_PATH / ".." / "vocabularies" / "dfo_nafc_instruments.csv"
# ).set_index("instrument_id")
global_attributes = {
    "Conventions": "CF-1.6,CF-1.7,CF-1.8,ACDD-1.3,IOOS 1.2",
    "naming_authority": "ca.gc.nafc",
}


def _int(value: str, null_values=None) -> int:
    """Attemp to convert string to int, return None if empty or failed"""
    if not value.strip():
        return
    try:
        value = int(value)
        if null_values and value in null_values:
            return None
        return value
    except TypeError:
        logger.error("Failed to convert string=%s to int", value)


def _float(value: str, null_values=None) -> float:
    """Attemp to convert string to float, return None if empty or failed"""
    if not value.strip():
        return
    try:
        value = float(value)
        if null_values and value in null_values:
            return None
        return value
    except TypeError:
        logger.error("Failed to convert string=%s to float", value)


def _get_dtype(var: str):
    return int if var == "scan" else float


def soft_catch_errors(function):
    def wrap(*args, **kwargs):
        try:
            value = function(*args, **kwargs)
            return value
        except ValueError:
            logger.error("Failed to parse %s", function.__name__, exc_info=True)
            return {}

    return wrap


@soft_catch_errors
def _parse_pfile_header_line1(line: str) -> dict:
    """Parse first row of the p file format which contains location and instrument information."""
    return dict(
        ship_code=line[:2],
        trip=_int(line[2:5]),
        station=_int(line[5:8]),
        latitude=_float(line[9:12]) + float(line[13:18]) / 60,
        longitude=_float(line[19:23]) + float(line[24:29]) / 60,
        time=pd.to_datetime(line[30:46], format="%Y-%m-%d %H:%M", utc=True),
        sounder_depth=_int(line[47:51])
        if line[47:51] not in ("9999", "0000")
        else None,  # water depth in meters 9999 or 0000 = not known
        instrument=line[
            52:57
        ],  # Sxxxxx is a seabird ctd XBTxx is an XBT for an XBT, A&C= Sippican probe, A&B mk9, B&D= Spartan probe, C&D mk12"
        set_number=_int(line[58:61]),  # usually same as stn
        cast_type=line[62],  # V vertical profile T for tow
        comment=line[62:78],
        card_1_id=line[79],
    )


@soft_catch_errors
def _parse_pfile_header_line2(line: str) -> dict:
    return dict(
        ship_code=line[:2],
        trip=_int(line[2:5]),
        station=_int(line[5:8]),
        scan_count=_int(line[9:15]),  # number of scan lines in file
        sampling_rate=_float(line[16:21]),  # 00.00 for unknown
        file_type=line[22],  # A for ASCII B for binary data
        channel_count=_int(
            line[24:26]
        ),  # number of data channels in file including dummies
        channel_ids=line[
            28:47
        ],  # id codes as described above in same order as channels
        channel_extra=line[47:58],
        cast_direction=line[59],  # "U for up cast only, D for down cast only, B bot"
        sub_interval=_int(
            line[61:64]
        ),  # sub sample interval, 000 if irregular stream or unknown"
        min_depth=_int(
            line[65:69]
        ),  # integral min depth in file from pres or depth channel
        max_depth=_int(
            line[70:74]
        ),  # integral max depth in file from pres or depth channel
        fishing_strata=_int(line[75:78]),  # ground fish specified strata number
        card_4_id=line[79],  # ,i1,4
    )


@soft_catch_errors
def _parse_pfile_header_line3(line: str) -> dict:
    """Parse P file 3 metadata line which present environment metadata"""
    return dict(
        ship_code=line[:2],
        trip=_int(line[2:5]),
        station=_int(line[5:8]),
        cloud=_int(line[9]),  # i1,
        wind_dir=_int(line[11:13]) * 10
        if line[11:13].strip() and line[11:13] != "99"
        else None,  # in 10 degree steps (eg 270 is=27)
        wind_speed_knots=_int(line[14:16]),  # i2,knots s= cale
        ww_code=_int(line[17:19]),  # i2,
        pressure_bars=_float(line[20:26], [-999.0, -999.9]),  # pressure mil-= bars
        air_dry_temp_celsius=_float(
            line[27:32], [-99.0, -99.9, 999.9]
        ),  # f5.1,tem= p °C
        air_wet_temp_celsius=_float(
            line[33:38], [-99.0, 99.9, -99.9, 999.9]
        ),  # f5.1,tem= p °C
        waves_period=_int(line[39:41]),  # i2,
        waves_height=_int(line[42:44]),  # i2,
        swell_dir=_int(line[45:47]) * 10 if line[45:47].strip() else None,  # i2,
        swell_period=_int(line[48:50]),  # i2,
        swell_height=_float(line[51:53]),  # i2,
        ice_conc=_int(line[54]),  # i1,
        ice_stage=_int(line[56]),  # i1,
        ice_bergs=_int(line[58]),  # i1,
        ice_SandT=_int(line[60]),  # i1,
        card_8_id=_int(line[79]),  # i1 ,=8
    )


def _parse_channel_stats(lines: list) -> dict:
    """Parse p file CHANNEL STATISTIC header section to cf variable dictionary"""

    def _get_range(attrs: dict) -> tuple:
        """Convert range to the variable dtype"""
        dtype = _get_dtype(attrs["name"])

        # Use int(float(x)) method because the integers have decimals
        func = (lambda x: int(float(x))) if dtype == int else float
        return tuple(
            map(
                func,
                [attrs["min"], attrs["max"]],
            )
        )

    if not lines:
        return {}
    read_stats = re.compile(
        r"# span\s+(?P<name>\w+)\s\=\s*(?P<min>[+-\.\d]+),\s*(?P<max>[+-\.\d]+)"
    )
    spans = [read_stats.search(line).groupdict() for line in lines if "span" in line]
    # Convert it to cf standard
    return {item["name"]: {"actual_range": _get_range(item)} for item in spans}


def _get_ship_code_metadata(shipcode: Union[int, str]) -> dict:
    shipcode = f"{shipcode:02g}" if isinstance(shipcode, int) else shipcode
    if p_file_shipcode["dfo_newfoundland_ship_code"].str.match(shipcode).any():
        return (
            p_file_shipcode.query(f"dfo_newfoundland_ship_code == '{shipcode}'")
            .iloc[0]
            .to_dict()
        )
    logger.warning("Unknown p-file shipcode=%s", shipcode)
    return {}


def _pfile_history_to_cf(lines: list) -> str:
    """Convert history to cf format: 2022-02-02T00:00:00Z - ...

    Args:
        lines (list): p file history list of strings

    Returns:
        str:
    """

    # """Convert history to cf format: 2022-02-02T00:00:00Z - ..."""

    history_timestamp = re.search(
        r"-- HISTORY --> (\w+ \w\w\w\s+\d+ \d{2}:\d{2}:\d{2} \d{4})", lines[0]
    )
    if not history_timestamp:
        logger.error("Failed to retrieve the history associated timestamp from header")
        return "".join(lines)

    timestamp = (
        pd.to_datetime(history_timestamp[1], format="%a %b %d %H:%M:%S %Y", utc=True)
        .isoformat()
        .replace("+00:00", "Z")
    )

    return "".join([f"{timestamp} - {line}" for line in lines[1:]])


def pfile(
    file: str,
    encoding: str = "UTF-8",
    rename_variables: bool = True,
    generate_extra_variables: bool = True,
) -> xr.Dataset:
    """Convert P-File to an xarray Dataset object

    Args:
        file (str): Path to pfile to parse

    Returns:
        xr.Dataset
    """

    def _check_ship_trip_stn():
        """Review if the ship,trip,stn string is the same
        accorss the 3 metadata rows
        """
        ship_trip_stn = [line[:9] for line in metadata_lines[1:]]
        if len(set(ship_trip_stn)) != 1:
            logger.error(
                "Ship,trip,station codes do not match" " in the header metadata: %s",
                ship_trip_stn,
            )
            raise RuntimeError(
                "Ship,trip,station codes do not match" " in the header metadata"
            )

    def _get_variable_vocabulary(variable: str) -> dict:
        matching_vocabulary = p_file_vocabulary.query(
            f"legacy_p_code == '{variable}' and "
            f"(accepted_instruments.isna() or "
            f"accepted_instruments in '{ds.attrs.get('instrument','')}' )"
        )
        if matching_vocabulary.empty:
            logger.warning("No vocabulary is available for variable=%s", variable)
            return []
        return matching_vocabulary.to_dict(orient="records")

    line = None
    header = {}
    section = None
    with open(file, encoding=encoding) as file_handle:
        # Read the four first lines to extract the information
        original_header = [file_handle.readline() for _ in range(4)]
        metadata_lines = original_header[:4]
        while file_handle:
            previous_line, line = line, file_handle.readline()
            if "-- DATA --" in line:
                break
            original_header += [line]

            # search section specific
            new_section = re.search(r"-- ([\w\s]+) -->", line)
            section = new_section[1] if new_section else section
            if section and "-- END --" in line:
                section = None
            if section is None:
                continue
            elif section not in header:
                header[section] = []

            header[section] += [line]

        # Define each fields width based on the column names
        names = re.findall(r"\w+", previous_line)

        # Read data section
        # TODO confirm that 5+12 character width is constant
        ds = pd.read_csv(
            file_handle,
            sep=r"\s+",
            engine="python",
            names=names,
            dtype={name: _get_dtype(name) for name in names},
        ).to_xarray()

    # Review datatypes
    if any([dtype == object for var, dtype in ds.dtypes.items()]):
        logger.warning(
            "Some columns dtype=object which suggest that the file data wasn't correctely parsed."
        )

    # Review metadata
    if metadata_lines[0] == "NAFC_Y2K_HEADER":
        raise RuntimeError(
            "File header doesn't contain pfile first line 'NAFC_Y2K_HEADER'"
        )
    _check_ship_trip_stn()

    # Convert dataframe to an xarray and populate information
    ds.attrs.update(
        {
            "id": metadata_lines[1][:8],
            **global_attributes,
            **_parse_pfile_header_line1(metadata_lines[1]),
            **_parse_pfile_header_line2(metadata_lines[2]),
            **_parse_pfile_header_line3(metadata_lines[3]),
            "history": header.get("HISTORY"),
        }
    )
    ds.attrs["original_header"] = "\n".join(original_header)
    ds.attrs["history"] = _pfile_history_to_cf(header.get("HISTORY"))
    ds.attrs.update(_get_ship_code_metadata(ds.attrs.get("ship_code", {})))

    # Move coordinates to variables:
    coords = ["time", "latitude", "longitude"]
    for coord in coords:
        if coord in ds.attrs:
            ds[coord] = ds.attrs[coord]
    ds = ds.set_coords([coord for coord in coords if coord in ds])

    # Populate variable attributes base on vocabulary
    variables_span = _parse_channel_stats(header.get("CHANNEL STATS"))
    extra_vocabulary_variables = []
    for var in ds.variables:
        ds[var].attrs.update(variables_span.get(var, {}))
        variable_attributes = _get_variable_vocabulary(var)
        if not variable_attributes:
            logger.warning("Missing vocabulary for p-file variable=%s", var)
            continue
        ds[var].attrs.update(variable_attributes[0])
        for extra in variable_attributes[1:]:
            extra_vocabulary_variables += [
                [
                    extra.pop("variable_name", var),
                    ds[var],
                    extra,
                ]
            ]

    # Rename variables
    if rename_variables:
        ds = ds.rename(
            {
                var: ds[var].attrs.pop("variable_name")
                for var in ds.variables
                if "variable_name" in ds[var].attrs
                and ds[var].attrs["variable_name"]
                and ds[var].attrs["variable_name"] != var
            }
        )

    # Generate extra variables
    if generate_extra_variables:
        for name, var, attrs in extra_vocabulary_variables:
            if name in ds:
                logger.warning(
                    (
                        "Extra variable is already in dataset and will be ignored. "
                        "name=%s, attrs=%s is already in dataset and will be ignored"
                    ),
                    var,
                    attrs,
                )
            apply_func = attrs.pop("apply_func", None)
            new_data = (
                eval(
                    apply_func,
                    {},
                    {
                        "gsw": gsw,
                        **{
                            ds[variable].attrs.get("legacy_p_code", variable): ds[
                                variable
                            ]
                            for variable in ds.variables
                        },
                    },
                )
                if apply_func not in (None, np.nan)
                else var
            )
            # TODO add to this history new variables generated
            ds[name] = (var.dims, new_data.data, {**var.attrs, **attrs})

    # standardize
    ds = standardize_dataset(ds)

    return ds
