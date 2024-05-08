"""
The Fisheries and Oceans Canada - Newfoundland and Labrador Region -
North Atlantic Fisheries Centre (NAFC) is a research facility located in St. John's, Newfoundland and Labrador.
"""

import inspect
import re
from functools import lru_cache
from pathlib import Path
from typing import Union

import gsw
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from ocean_data_parser.parsers import seabird
from ocean_data_parser.parsers.utils import standardize_dataset
from ocean_data_parser.vocabularies.load import (
    dfo_nafc_p_file_vocabulary,
    dfo_platforms,
)

MODULE_PATH = Path(__file__).parent
p_file_vocabulary = dfo_nafc_p_file_vocabulary()
p_file_shipcode = dfo_platforms().drop(columns=["accepted_platform_name"])

global_attributes = {
    "Conventions": "CF-1.6,CF-1.7,CF-1.8,ACDD-1.3,IOOS 1.2",
    "naming_authority": "ca.gc.nafc",
}

METQA_ATTRS_MAPPING = {
    "wind_speed": "wind_speed_knots",
    "bar_pressure": "pressure_bars",
    "air_temp_dry": "air_dry_temp_celsius",
    "air_temp_wet": "air_wet_temp_celsius",
    "bergs": "ice_bergs",
    "sit_and_trend": "ice_sit_and_trend",
}

METQA_DTYPES = {
    "wind_speed_knots": pd.Int64Dtype(),
    "wind_dir": pd.Int64Dtype(),
    "pressure_bars": float,
    "air_dry_temp_celsius": float,
    "air_wet_temp_celsius": float,
    "swell_dir": pd.Int64Dtype(),
    "swell_period": pd.Int64Dtype(),
    "swell_height": float,
    "ice_conc": pd.Int64Dtype(),
    "ice_stage": pd.Int64Dtype(),
    "ice_bergs": pd.Int64Dtype(),
    "ice_sit_and_trend": pd.Int64Dtype(),
}


def _traceback_error_line():
    current_frame = inspect.currentframe()
    previous_frame = current_frame.f_back.f_back
    line_number = previous_frame.f_lineno
    cmd_line = Path(__file__).read_text().split("\n")[line_number - 1].strip()
    return f"<{previous_frame.f_code.co_name} line {line_number}>: {cmd_line}"


def _int(value: str, null_values=None, level="WARNING", match: str = None) -> int:
    """Attemp to convert string to int, return None if empty or failed"""
    if not value or not value.strip() or value in (null_values or []):
        return
    if match:
        new_value = re.match(match, value)
        if new_value:
            value = [item for item in new_value.groups() if item][0]
        else:
            logger.log(
                level,
                "Failed to convert: {} => int('{}')",
                _traceback_error_line(),
                value,
            )
            return pd.NA
    if null_values and value in null_values:
        return pd.NA
    elif value in ("0.", ".0"):
        return 0
    try:
        value = int(value)
        if null_values and value in null_values:
            return pd.NA
        return int(value)
    except ValueError:
        logger.log(
            level,
            "Failed to convert: {} => int('{}')",
            _traceback_error_line(),
            value,
        )
        return pd.NA


def _float(value: str, null_values=None, level="WARNING") -> float:
    """Attemp to convert string to float, return None if empty or failed"""
    if not value or not value.strip() or value in (null_values or []):
        return
    try:
        value = float(value)
        return None if null_values and value in null_values else value
    except ValueError:
        logger.log(
            level,
            "Failed to convert: {} => float('{}')",
            _traceback_error_line(),
            value,
        )
        return pd.NA


def _get_dtype(var: str):
    return int if var == "scan" else float


def _parse_ll(deg: float, min: float) -> float:
    """Combine deg and min values from latitude and longitude to decimal degrees"""
    if pd.isna(deg) or pd.isna(min):
        return
    dir = -1 if deg < 0 else 1
    return deg + (dir * min / 60)


def _parse_pfile_header_line1(line: str) -> dict:
    """Parse first row of the p file format which contains location and instrument information."""

    if line[44:46] == "60":
        # Fix some dates are using 60 minutes which is not compatible with pandas datetime
        dt = pd.Timedelta("1min")
        line = line[:44] + "00" + line[46:]
    else:
        dt = pd.Timedelta("0")

    return dict(
        # ship_code=line[:2],
        # trip=_int(line[2:5], level="ERROR"),
        # station=_int(line[5:8], level="ERROR"),
        latitude=_parse_ll(
            _float(line[9:12], level="ERROR"), _float(line[13:18], level="ERROR")
        ),
        longitude=_parse_ll(
            _float(line[19:23], level="ERROR"), _float(line[24:29], level="ERROR")
        ),
        time=pd.to_datetime(line[30:46], format="%Y-%m-%d %H:%M", utc=True) + dt,
        sounder_depth=_int(
            line[47:51], ["9999", "0000", "-999"]
        ),  # water depth in meters
        instrument=line[52:57],  # see note below
        set_number=_int(
            line[58:61],
            ("SET", "xxx", "set", "XXX", "ctd", "xbt", "Stn", "STN", "stn", "nil"),
            match=r"\s*(\d+)\.?|[sS](\d+)|[sS][tT](\d+)|\#(\d+)",
        ),  # usually same as stn
        cast_type=line[62],  # V vertical profile T for tow
        comment=line[62:78],
        card_1_id=line[79],
    )
    # instrument = #
    #   Sxxxxx is a seabird ctd
    #   XBTxx is an XBT for an XBT,
    #   A&C= Sippican probe,
    #   A&B mk9, B&D= Spartan probe,
    #   C&D mk12


def _parse_pfile_header_line2(line: str) -> dict:
    return dict(
        # ship_code=line[:2],
        # trip=_int(line[2:5]),
        # station=_int(line[5:8]),
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


def _parse_pfile_header_line3(line: str) -> dict:
    """Parse P file 3 metadata line which present environment metadata"""
    if not line or not line.strip() or len(line) < 79:
        logger.warning("Missing pfile header line 3")
        return {}
    return dict(
        # ship_code=line[:2],
        # trip=_int(line[2:5]),
        # station=_int(line[5:8]),
        cloud=_int(line[9], null_values=["x", "-", "/", "."]),  # i1,
        wind_dir=_int(line[11:13]) * 10
        if line[11:13].strip() and line[11:13] not in ["99", "98"]
        else None,  # in 10 degree steps (eg 270 is=27)
        wind_speed_knots=_int(
            line[14:16],
            null_values=[
                "99",
            ],
        ),  # i2,knots s= cale
        ww_code=_int(line[17:19]),  # i2,
        pressure_bars=_float(line[20:26], [-999.0, -999.9]),  # pressure mil-= bars
        air_dry_temp_celsius=_float(
            line[27:32], [-99.0, -99.9, 999.9, -49.9]
        ),  # f5.1,tem= p °C
        air_wet_temp_celsius=_float(
            line[33:38], [-99.0, 99.9, -99.9, 999.9, -49.9]
        ),  # f5.1,tem= p °C
        waves_period=_int(
            line[39:41], null_values=["XX", "- ", "99"], match=r"\s*(\d+)\.?"
        ),  # i2,
        waves_height=_float(
            line[42:44],
            null_values=[
                "99",
            ],
        ),  # i2,
        swell_dir=_int(line[45:47]) * 10
        if line[45:47].strip()
        and line[45:47]
        not in [
            "99",
        ]
        else None,  # i2,
        swell_period=_int(line[48:50], null_values=["XX", "- ", "99"]),  # i2,
        swell_height=_float(
            line[51:53],
            null_values=[
                "99",
            ],
        ),  # i2,
        ice_conc=_int(line[54]),  # i1,
        ice_stage=_int(line[56]),  # i1,
        ice_bergs=_int(line[58]),  # i1,
        ice_sit_and_trend=_int(line[60]),  # i1,
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


def _get_platform_by_nafc_platform_code(platform_code: Union[int, str]) -> dict:
    platform_code = (
        f"{platform_code:02g}"
        if isinstance(platform_code, int)
        else platform_code.upper()
    )
    if p_file_shipcode["dfo_nafc_platform_code"].str.match(platform_code).any():
        return (
            p_file_shipcode.query(f"dfo_nafc_platform_code == '{platform_code}'")
            .iloc[0]
            .to_dict()
        )
    logger.warning("Unknown dfo_nafc_platform_code={}", platform_code)
    return {}


def _get_platform_by_nafc_platform_name(platform_name: str) -> dict:
    if platform_name in p_file_shipcode["dfo_nafc_platform_name"].tolist():
        return (
            p_file_shipcode.query(f"dfo_nafc_platform_name == '{platform_name}'")
            .iloc[0]
            .to_dict()
        )
    logger.warning("Unknown dfo_nafc_platform_name={}", platform_name)
    return {}


def _pfile_history_to_cf(lines: list) -> str:
    """Convert history to cf format: 2022-02-02T00:00:00Z - ...

    Args:
        lines (list): p file history list of strings

    Returns:
        str:
    """

    # """Convert history to cf format: 2022-02-02T00:00:00Z - ..."""
    if not lines:
        return ""

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


def _get_pfile_variable_vocabulary(variable: str, instrument: str = None) -> dict:
    """Retrieve variable vocabulary"""
    if variable == "xxx":
        return []
    matched_legacy_p_code = p_file_vocabulary.apply(
        lambda x: re.fullmatch(x["legacy_p_code"], variable, re.IGNORECASE), axis=1
    ).notna()

    if not any(matched_legacy_p_code):
        logger.warning("No vocabulary is available for variable={}", variable)
        return []

    matching_vocabulary = p_file_vocabulary.loc[matched_legacy_p_code].query(
        f"(accepted_instruments.isna() or "
        f"accepted_instruments in '{instrument or ''}' )"
    )
    if matching_vocabulary.empty:
        logger.warning("No vocabulary is available for variable={}", variable)
        return []
    return matching_vocabulary.to_dict(orient="records")


def pfile(
    file: str,
    encoding: str = "UTF-8",
    rename_variables: bool = True,
    generate_extra_variables: bool = True,
    encoding_errors: str = "strict",
) -> xr.Dataset:
    """Parse DFO NAFC oceanography p-file format

    Args:
        file (str): file path
        encoding (str, optional): file encoding. Defaults to "UTF-8".
        rename_variables (bool, optional): Rename variables to BODC
            standard. Defaults to True.
        generate_extra_variables (bool, optional): Generate extra
            BODC mapping variables. Defaults to True.

    Raises:
        TypeError: File provided isn't a p file.

    Returns:
        xr.Dataset: Parser dataset
    """

    def _parse_ship_trip_stn():
        """Review if the ship,trip,stn string is the same
        accorss the 3 metadata rows and the file name."""
        ship_trip_stn = [line[:8] for line in metadata_lines[1:] if line.strip()] + [
            file.stem
        ]
        if len(set(ship_trip_stn)) != 1:
            logger.warning(
                "Ship,trip,station isn't consistent: {}. "
                "The file name will be considered = {}.",
                set(ship_trip_stn),
                file.stem,
            )
        return dict(
            ship_code=file.stem[:2],
            trip=int(file.stem[2:5]),
            station=int(file.stem[5:8]),
        )

    file = Path(file)
    line = None
    header = {}
    section = None
    with open(file, encoding=encoding, errors=encoding_errors) as file_handle:
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
        if len(names) != len(set(names)):
            # Rename duplicated names
            logger.warning("Column names aren't unique: {}", names)
            duplicated_names = []
            for index, name in enumerate(names):
                if names.count(name) > 1:
                    new_name = f"{name}{names[:index].count(name)}"
                    logger.warning("Rename {} to {}", name, new_name)
                    duplicated_names += [new_name]
                else:
                    duplicated_names += [name]

            names = duplicated_names

        # Read data section
        # TODO confirm that 5+12 character width is constant
        ds = pd.read_csv(
            file_handle,
            sep=r"\s+",
            engine="python",
            names=names,
            dtype={name: _get_dtype(name) for name in names},
            encoding_errors=encoding_errors,
        ).to_xarray()

    if len(ds.index) == 0:
        logger.error("No data found in file")

    # Review datatypes
    if any([dtype == object for _, dtype in ds.dtypes.items()]):
        logger.warning(
            "Some columns dtype=object suggest the file data wasn't correctely parsed."
        )

    # Review metadata
    if metadata_lines[0] == "NAFC_Y2K_HEADER":
        raise TypeError(
            "File header doesn't contain pfile first line 'NAFC_Y2K_HEADER'"
        )

    # Convert dataframe to an xarray and populate information
    ds.attrs.update(
        {
            "id": metadata_lines[1][:8],
            **global_attributes,
            **_parse_ship_trip_stn(),
            **(_parse_pfile_header_line1(metadata_lines[1]) or {}),
            **(_parse_pfile_header_line2(metadata_lines[2]) or {}),
            **(_parse_pfile_header_line3(metadata_lines[3]) or {}),
            "history": header.get("HISTORY"),
        }
    )
    ds.attrs["original_header"] = "\n".join(original_header)
    ds.attrs["history"] = _pfile_history_to_cf(header.get("HISTORY"))
    ds.attrs.update(_get_platform_by_nafc_platform_code(ds.attrs.get("ship_code", {})))

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
        variable_attributes = _get_pfile_variable_vocabulary(
            var, ds.attrs.get("instrument")
        )
        if not variable_attributes:
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
                logger.info(
                    (
                        "Extra variable is already in dataset and will be ignored. "
                        "name={}, attrs={} is already in dataset and will be ignored"
                    ),
                    var.name,
                    attrs,
                )
                continue
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
            ds.attrs[
                "history"
            ] += f"\n{pd.Timestamp.now()} - Generated variable {name} = {apply_func}"
            attrs["source"] = f"Generated variable {name} = {apply_func}"
            ds[name] = (var.dims, new_data.data, {**var.attrs, **attrs})

    # standardize
    ds = standardize_dataset(ds)

    return ds


@logger.catch
def _parse_lat_lon(latlon: str) -> float:
    """Parse latitude and longitude string to float"""
    if not latlon:
        logger.error("No latitude or longitude provided")
        return
    deg, min, dir = latlon.split(" ")
    return (-1 if dir in ("S", "W") else 1) * (int(deg) + float(min) / 60)


@logger.catch
@lru_cache
def _get_metqa_table(file) -> pd.DataFrame:
    """Load NAFC metqa table which contains each files assoicated weather data"""
    df = pd.read_csv(file, sep="\s*\,", engine="python")
    df.columns = [
        col.lower().split("[")[0].strip().replace(" ", "_") for col in df.columns
    ]
    df["time"] = pd.to_datetime(df["date"] + " " + df["time"], utc=True)
    df["latitude"] = df["latitude"].apply(lambda x: _parse_lat_lon(x))
    df["longitude"] = df["longitude"].apply(lambda x: _parse_lat_lon(x))
    df = df.drop(columns=["date"])
    df = df.rename(columns=METQA_ATTRS_MAPPING).astype(METQA_DTYPES)
    return df


def _add_metqa_info_to_pcvn(file: Path) -> Path:
    """Find the matching metqa table to the pcnv file"""

    glob_expression = f"{file.stem.rsplit('_',1)[0]}_metqa_*.csv"
    metqa_file = list(file.parent.glob(glob_expression))
    if metqa_file and len(metqa_file) == 1:
        df = _get_metqa_table(metqa_file[0])
        metadata = df.query(f"station == '{file.stem}'")
        if metadata.empty:
            logger.warning("No station={} in metqa file={}", file.stem, metqa_file)
            return {}
        return metadata.iloc[0].dropna().to_dict()
    elif metqa_file and len(metqa_file) > 1:
        logger.error(
            "Multiple metqa files found={} for path={},glob={}",
            metqa_file,
            file.parent,
            glob_expression,
        )
    else:
        logger.warning(
            "No metqa table file path={},glob={}", file.parent, glob_expression
        )
    return {}


def pcnv(
    path: Path,
    rename_variables: bool = True,
    generate_extra_variables: bool = True,
    global_attributes: dict = None,
    encoding: str = "UTF-8",
    encoding_errors: str = "strict",
    match_metqa_table: bool = False,
) -> xr.Dataset:
    """DFO NAFC pcnv file format parser

    The pcnv format  essentially a seabird cnv file format
    with NAFC specific inputs within the manual section.

    Args:
        path (Path): pcvn file path
        rename_variables (bool, optional): Rename variables to
            DFO BODC names. Defaults to True.
        generate_extra_variables (bool, optional): Generate extra
            vocabulary variables. Defaults to True.
        global_attributes (dict, optional): Global attributes to add to the dataset.
        match_metqa_table (bool, optional): Match metqa table to the file if
            available within same directory. Defaults to True.

    Returns:
        xr.Dataset
    """

    def _pop_attribute_from(names: list):
        """Pop attribute from dataset"""
        for name in names:
            if name in ds.attrs:
                return ds.attrs.pop(name)

        if "comment" not in names[0].lower():
            logger.error("No matching attribute found in {}", names)

    def get_vocabulary(**kwargs):
        return p_file_vocabulary.query(
            " and ".join(f"{key} == '{value}'" for key, value in kwargs.items())
        ).to_dict(orient="records")

    path = Path(path)
    ds = seabird.cnv(
        path,
        encoding=encoding,
        encoding_errors=encoding_errors,
        xml_parsing_error_level="WARNING",
    )

    # Map global attributes
    ship_trip_seq_station = re.search(
        r"(?P<dfo_nafc_platform_name>\w{3})(?P<trip>\d{3})_(?P<year>\d{4})_(?P<stn>\d{3})",
        ds.attrs.pop("vessel_trip_seq_stn", ""),
    )
    if not ship_trip_seq_station:
        logger.error(
            "Unable to parse ship_trip_seq_station from VESEL/TRIP/SEQ STN= {}",
            ds.attrs.get("vessel_trip_seq_stn", ""),
        )
    attrs = {
        "dfo_nafc_platform_name": ship_trip_seq_station["dfo_nafc_platform_name"],
        **_get_platform_by_nafc_platform_name(
            ship_trip_seq_station["dfo_nafc_platform_name"]
        ),
        "trip": _int(ship_trip_seq_station["trip"]),
        "year": _int(ship_trip_seq_station["year"]),
        "station": _int(ship_trip_seq_station["stn"]),
        "time": pd.to_datetime(ds.attrs.pop("date_time"), utc=True),
        "latitude": _parse_lat_lon(
            _pop_attribute_from(
                ["latitude", "latitude_xx_xx.xx", "latitude_xx_xx.xx_n"]
            )
        ),
        "longitude": _parse_lat_lon(
            _pop_attribute_from(
                ["longitude", "longitude_xx_xx.xx", "longitude _xx_xx.xx_w"]
            )
        ),
        "sounder_depth": ds.attrs.pop("sounding_depth_m", None),
        "instrument": ds.attrs.pop("probe_type", None),
        "set_number": _int(ds.attrs.pop("xbt_number", None))
        or _int(ds.attrs.pop("ctd_number", None)),
        "format": ds.attrs.pop("format", None),
        "commment": _pop_attribute_from(["comments", "comments_14_char"]),
        "trip_tag": ds.attrs.pop("trip_tag", None),
        "vnet": ds.attrs.pop("vnet", None),
        "do2": ds.attrs.pop("do2", None),
        "bottles": _int(ds.attrs.pop("bottles", None)),
        **(global_attributes or {}),
    }

    # load metqa table attributes
    if match_metqa_table:
        attrs.update(_add_metqa_info_to_pcvn(path))

    ds.attrs.update(attrs)

    # Move coordinates to variables
    coords = ["time", "latitude", "longitude"]
    p_file_vocabulary
    for coord in coords:
        if coord in ds.attrs:
            ds[coord] = ds.attrs[coord]
            ds[coord].attrs = get_vocabulary(variable_name=coord)[0]

    ds = ds.set_coords([coord for coord in coords if coord in ds])

    # Map variable attributes to pfile vocabulary via long_name
    variables_new_name = {}
    for variable in ds.variables:
        if variable in coords:
            continue
        variable_attributes = get_vocabulary(
            long_name=ds[variable].attrs.get("long_name", variable)
        )
        if not variable_attributes:
            logger.warning("Missing vocabulary for variable={}", variable)
            continue

        if rename_variables and variable_attributes[-1].get("variable_name"):
            variables_new_name[variable] = variable_attributes[-1].pop("variable_name")

        ds[variable].attrs.update(variable_attributes[-1])
        if not generate_extra_variables:
            continue

        for extra in variable_attributes[0:-1]:
            new_var = extra.pop("variable_name", variable)
            if new_var == "depth" and "depSM" in ds.variables:
                continue
            logger.debug("Generate extra variable={}", new_var)
            ds[new_var] = (ds[variable].dims, ds[variable].data, extra)

    if rename_variables:
        logger.debug("Rename variables to NAFC standard: {}", variables_new_name)
        ds = ds.rename(variables_new_name)

    ds = standardize_dataset(ds)

    return ds
