import pandas as pd
import json
import re
import logging
from .utils import test_parsed_dataset

logger = logging.getLogger(__name__)

header_end = "[Data]\n"
van_essen_variable_mapping = {
    "PRESSURE": "pressure",
    "TEMPERATURE": "temperature",
    "CONDUCTIVITY": "conductivity",
    "SPEC.COND.": "specific_conductance",
}


def MON(
    file_path,
    output=None,
    standardize_variable_names=True,
    kwargs_input_read_csv=None,
):
    """
    Read MON file format from Van Essen Instrument format.
    :param errors: default ignore
    :param encoding: default UTF-8
    :param file_path: path to file to read
    :return: metadata dictionary dataframe
    """
    # MON File Header end
    header_end = "[Data]\n"

    def date_parser(time):
        return pd.to_datetime(f"{time} {timezone}", utc=True)

    if kwargs_input_read_csv is None:
        kwargs_input_read_csv = {}
    with open(
        file_path,
        encoding=kwargs_input_read_csv.get("encoding"),
        errors=kwargs_input_read_csv.get("encoding_errors"),
    ) as fid:
        line = ""
        section = "header_info"
        info = {section: {}}
        while not line.startswith(header_end):
            # Read line by line
            line = fid.readline()
            if re.match("\[.+\]", line):
                section = re.search("\[(.+)\]", line)[1]
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
            id = re.search(r"Channel (\d+) from data header", key)
            if id:
                info["Channel"][items["Identification"]] = items
                info["Channel"][items["Identification"]]["id"] = int(id[1])

        # Define column names
        channel_names = ["time"] + [
            attrs["Identification"] for id, attrs in info["Channel"].items()
        ]
        # Read the rest with pandas
        # Find first how many records exist
        info["n_records"] = int(fid.readline())

        # Retrieve timezone
        timezone = (
            re.search("UTC([\-\+]*\d+)", info["Series settings"]["Instrument number"])[
                1
            ]
            + ":00"
        )

        # Read data (Seperator is minimum 2 spaces)
        df = pd.read_csv(
            fid,
            names=channel_names,
            header=None,
            sep="\s\s+",
            skipfooter=1,
            engine="python",
            comment="END OF DATA FILE OF DATALOGGER FOR WINDOWS",
            parse_dates=["time"],
            date_parser=date_parser,
        )

    # If there's less data then expected send a warning
    if len(df) < info["n_records"]:
        assert RuntimeWarning(
            f'Missing data, expected {info["n_records"]} and found only {len(df)}'
        )
    # Convert to xarray
    ds = df.to_xarray()

    # Ignore column number in variable names
    ds = ds.rename({var: re.sub("^\d+\:\s*", "", var) for var in ds})

    # IF PRESSURE in cm, convert to meter
    if "PRESSURE" in ds and "cm" in info["Channel"]["PRESSURE"]["Range"]:
        logger.warning("Convert Pressure from cm to m")
        ds["PRESSURE"] = ds["PRESSURE"] / 100

    # Add Conductivity if missing
    if "CONDUCTIVITY" not in ds and "SPEC.COND." in ds:
        ds["CONDUCTIVITY"] = specific_conductivity_to_conductivity(
            ds["SPEC.COND."], ds["TEMPERATURE"]
        )

    # Specific Conductance if missing
    if "CONDUCTIVITY" in ds and "SPEC.COND." not in ds:
        ds["SPEC.COND."] = conductivity_to_specific_conductivity(
            ds["CONDUCTIVITY"], ds["TEMPERATURE"]
        )

    # Reformat metadata to CF/ACDD standard
    ds.attrs = {
        "instrument_manufacturer": "Van Essen Instruments",
        "instrument_type": info["Logger settings"]["Instrument type"],
        "instrument_sn": info["Logger settings"]["Serial number"],
        "time_coverage_resolution": info["Logger settings"]["Sample period"],
        "original_metadata": json.dumps(info),
    }
    # Standardize variables
    if standardize_variable_names:
        ds = ds.rename(van_essen_variable_mapping)

    # Run tests on parsed data
    test_parsed_dataset(ds)

    # Output
    if output == "dataframe":
        df = ds.to_pandas()
        for var in ["instrument_manufacturer", "instrument_type", "instrument_sn"][
            ::-1
        ]:
            df.insert(0, var, ds.attrs[var])
        return df
    return ds


def specific_conductivity_to_conductivity(
    spec_cond, temp, theta=1.91 / 100, temp_ref=25
):
    return (100 + theta * (temp - temp_ref)) / 100 * spec_cond


def conductivity_to_specific_conductivity(cond, temp, theta=1.91 / 100, temp_ref=25):
    return 100 / (100 + theta * (temp - temp_ref)) * cond
