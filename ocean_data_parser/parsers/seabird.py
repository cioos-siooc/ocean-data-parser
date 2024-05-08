"""
This page provides functions for parsing data files in Seabird Scientific format.
The Seabird Scientific format is commonly used for oceanographic data collection
and is supported by [Seabird Scientific](https://www.seabird.com).
"""

import difflib
import json
import logging
import re
from datetime import datetime

import pandas as pd
import xarray
import xmltodict
from pyexpat import ExpatError

from ocean_data_parser.parsers.utils import convert_datetime_str, standardize_dataset
from ocean_data_parser.vocabularies.load import seabird_vocabulary

logger = logging.getLogger(__name__)

var_dtypes = {
    "date": str,
    "bottle": str,
    "stats": str,
    "scan": int,
}
SBE_TIME_FORMAT = "%b %d %Y %H:%M:%S"  # Jun 23 2016 13:51:30
sbe_time = re.compile(
    r"(?P<time>\w\w\w\s+\d{1,2}\s+\d{1,4}\s+\d\d\:\d\d\:\d\d)(?P<comment>.*)"
)

seabird_variable_attributes = seabird_vocabulary()

IGNORED_HEADER_LINES = [
    "* GetHD\n",
    "* GetSD\n",
    "* GetDS\n",
    "* GetDH\n",
    "* GetCD\n",
    "* GetCC\n",
    "* GetEC\n",
    "* HD\n",
    "* SD\n",
    "* DS\n",
    "* DH\n",
    "* CD\n",
    "* CC\n",
    "* EC\n",
    "* S>\n",
    "*\n",
]

SBE_DATA_PROCESSING_MODULES = [
    "datcnv",
    "filter",
    "alignctd",
    "celltm",
    "loopedit",
    "derive",
    "Derive",
    "DeriveTEOS_10",
    "binavg",
    "split",
    "strip",
    "section",
    "wild",
    "window",
    "bottlesum",
]
is_seabird_processing_stage = re.compile(
    rf"\# (?P<module>{'|'.join(SBE_DATA_PROCESSING_MODULES)})"
    r"_(?P<parameter>[^\s\:]+)( = |: )(?P<value>.*)"
)


def _convert_to_netcdf_var_name(var_name):
    """Convert seabird variable name to a netcdf compatible format."""
    return var_name.replace("/", "Per")


def _add_seabird_vocabulary(variable_attributes: dict) -> dict:
    for var in variable_attributes.keys():
        var_lower = var.lower()
        if var_lower in seabird_variable_attributes:
            variable_attributes[var].update(seabird_variable_attributes[var_lower])
        elif (
            var.endswith("_sdev")
            and var_lower[:-5] in seabird_variable_attributes.keys()
        ):
            variable_attributes[var].update(seabird_variable_attributes[var_lower[:-5]])
        else:
            logger.warning("Variable %s is missing from vocabulary dictionary", var)
    return variable_attributes


def cnv(
    file_path: str,
    encoding: str = "UTF-8",
    encoding_errors="strict",
    xml_parsing_error_level="ERROR",
    generate_instrument_variables: bool = False,
    save_orginal_header: bool = False,
) -> xarray.Dataset:
    """Parse Seabird CNV format

    Args:
        file_path (str): file path
        encoding (str, optional): encoding to use. Defaults to "UTF-8".
        xml_parsing_error_level (str, optional): Error level for XML parsing.
            Defaults to "ERROR".
        generate_instrument_variables (bool, optional): Generate instrument
            variables following the IOOS 1.2 standard. Defaults to False.

    Returns:
        xarray.Dataset: Dataset
    """
    """Import Seabird cnv format as an xarray dataset."""

    with open(file_path, encoding=encoding, errors=encoding_errors) as f:
        header = _parse_seabird_file_header(
            f, xml_parsing_error_level=xml_parsing_error_level
        )
        header["variables"] = _add_seabird_vocabulary(header["variables"])
        df = pd.read_csv(
            f,
            delimiter=r"\s+",
            names=header["variables"].keys(),
            dtype={
                var: var_dtypes.get(var, float) for var in header["variables"].keys()
            },
            na_values=["-1.#IO", "-9.99E-29"],
            encoding_errors=encoding_errors,
        )

    header = _generate_seabird_cf_history(header)

    ds = _convert_sbe_dataframe_to_dataset(df, header)
    if generate_instrument_variables:
        ds, _ = _generate_instruments_variables_from_xml(ds, header["seabird_header"])
    if not save_orginal_header:
        ds.attrs.pop("seabird_header")
    return standardize_dataset(ds)


def btl(
    file_path: str,
    encoding: str = "UTF-8",
    xml_parsing_error_level="ERROR",
    save_orginal_header: bool = False,
) -> xarray.Dataset:
    """Parse Seabird BTL format

    Args:
        file_path (str): file path
        encoding (str, optional): Encoding to use. Defaults to "UTF-8".
        xml_parsing_error_level (str, optional): Error level for XML parsing. Defaults to "ERROR".

    Returns:
        xarray.Dataset: Dataset
    """

    with open(file_path, encoding=encoding) as f:
        header = _parse_seabird_file_header(
            f, xml_parsing_error_level=xml_parsing_error_level
        )

        # Retrieve variables from bottle header and lower the first letter of each variable
        variable_list = [
            var[0].lower() + var[1:] for var in header["bottle_columns"]
        ] + ["stats"]
        df = pd.read_fwf(
            f,
            widths=[10, 12] + [11] * (len(header["bottle_columns"]) - 1),
            names=variable_list,
            dtype={var: var_dtypes.get(var, float) for var in variable_list},
        )

    # Split statistical data info separate dateframes
    df["bottle"] = df["bottle"].ffill().astype(int)
    df["stats"] = df["stats"].str.extract(r"\((.*)\)")
    df = df.set_index("bottle")
    df_grouped = df.query("stats=='avg'")
    for stats in df.query("stats!='avg'")["stats"].drop_duplicates().to_list():
        df_grouped = df_grouped.join(df.query("stats==@stats").add_suffix(f"_{stats}"))
    df = df_grouped

    # Generate time variable
    df["time"] = pd.to_datetime(df.filter(like="date").apply(" ".join, axis="columns"))

    # Ignore extra variables
    drop_columns = [col for col in df if re.search("^date|^stats|^bottle_", col)]
    df = df.drop(columns=drop_columns)

    # Improve metadata
    n_scan_per_bottle = int(header["processing"][0]["scans_per_bottle"])
    header = _generate_seabird_cf_history(header)

    # Retrieve vocabulary associated with each variables
    header["variables"] = {var: header["variables"].get(var, {}) for var in df.columns}
    header["variables"] = _add_seabird_vocabulary(header["variables"])

    # Convert to xarray
    ds = _convert_sbe_dataframe_to_dataset(df, header)

    # Add cell_method attribute
    for var in ds:
        if var.endswith("_sdev") and var[:-5] in ds:
            ds[var].attrs[
                "cell_method"
            ] = f"scan: standard_deviation (previous {n_scan_per_bottle} scans)"
            # TODO confirm that seabird uses the previous records from this timestamp
        elif var not in ["time", "bottle"]:
            ds[var].attrs[
                "cell_method"
            ] = f"scan: mean (previous {n_scan_per_bottle} scans)"

    if not save_orginal_header:
        ds.attrs.pop("seabird_header")
    return standardize_dataset(ds)


def _convert_sbe_dataframe_to_dataset(df, header):
    """Convert Parsed DataFrame to a dataset"""
    # Convert column names to netcdf compatible format
    df.columns = [_convert_to_netcdf_var_name(var) for var in df.columns]
    header["variables"] = {
        _convert_to_netcdf_var_name(var): attrs
        for var, attrs in header["variables"].items()
    }

    ds = df.to_xarray()
    variable_attributes = header.pop("variables")
    for var, attrs in variable_attributes.items():
        if var not in ds:
            continue
        ds[var].attrs = attrs
    ds.attrs = header
    return ds


def _parse_seabird_file_header(f, xml_parsing_error_level="ERROR"):
    """Parsed seabird file headers"""

    def standardize_attribute(attribute):
        attribute = re.sub(r"\s+|\(|\||\)|\/", "_", attribute.strip()).lower()
        attribute = re.sub(r"\_+", "_", attribute)
        attribute = re.sub(r"\_+$", "", attribute)
        return attribute

    def read_comments(line):
        """Read comments(**) in seabird header"""
        if re.match(r"\*\* .*(\:|\=).*", line):
            result = re.match(r"\*\* (?P<key>[^:=]*)(\:|\=)(?P<value>.*)", line)
            key, _, value = result.groups()
            # Standardize key to match NetCDF requirements
            key = standardize_attribute(key)

            if key.strip() in header:
                # append string to existing key on a new line
                header[key] += "\n" + value.strip()
            else:
                header[key] = value.strip()
        elif line.startswith("** QA Applied:"):
            # This could be specific to DFO NAFC PCNV format
            header["processing"].append(
                {"module": "QA applied", "message": line.split(":", 1)[1]}
            )
        else:
            header["comments"] += [line[2:]]

    def read_asterisk_line(line):

        if line.startswith((r"* Sea-Bird", r"* SBE ")) and not line.startswith("* SBE 38 = "):
            instrument_type = re.search(
                r"\* Sea-Bird (.*) Data File\:?|\* SBE (.*)", line
            ).groups()
            header["instrument_type"] += "".join(
                [item for item in instrument_type if item]
            )
        elif re.match(r"\* Turo XBT Data File:", line):
            header["instrument_type"] += "Turo XBT"
        elif re.match(r"\* Software version .*", line, re.IGNORECASE):
            header["software_version"] = re.search(
                r"\* Software version (.*)", line, re.IGNORECASE
            )[1]
        elif (
            line.startswith(("* advance", "* delete", "* test","* autorun","* number of scans to average"))
            or "added to scan" in line
        ):
            header["processing"].append({"module": "on-instrument", "message": line[2:]})
        elif line.startswith("* SeacatPlus V"):
            header["instrument_firmware"] = line[10:].split("SERIAL")[0].strip()
        elif line.startswith("* cast"):
            header["processing"].append({"module": "cast", "message": line[2:]})
        elif sensor_calibration := re.match(
            r"\* (?P<variable>temperature|conductivity|pressure|rtc):\s*(?P<calibration_date>\d\d-\w\w\w-\d\d)",
            line,
        ):
            header["calibration"][sensor_calibration["variable"]] = {
                "calibration_date": sensor_calibration["calibration_date"]
            }
        elif pressure_sensor := re.match(r"\* pressure sensor = (?P<type>[\w\s]+), range = (?P<range>.*)", line):
            if "pressure" not in header["calibration"]:
                header["calibration"]["pressure"] = {}
            header["calibration"]["pressure"].update(pressure_sensor.groupdict())
        elif pressure_sensor := re.match(r"\* pressure S\/N = (?P<serial_number>\d+), range = (?P<range>[^:]):(?P<calibration_date>.+)", line):
            if "pressure" not in header["calibration"]:
                header["calibration"]["pressure"] = {}
            header["calibration"]["pressure"].update(pressure_sensor.groupdict())
        elif volt_calibration := re.match(r"\* volt\s*(?P<channel>\d)+:\s(?P<extra>.*)", line):
            header["calibration"][f"volt {volt_calibration['channel']}"] = dict(
                [item.split(" = ") for item in volt_calibration["extra"].split(", ")]
            )

        elif re.match(r"\*\s{4,}[A-Z0-9]+ = [0-9\.e\-\+]+", line):
            attr, value = line[2:].split(" = ", 1)
            # Retrieve the last sensor added to calibration
            sensor = list(header["calibration"].keys())[-1]

            header["calibration"][sensor][attr.strip()] = float(value.strip())
        elif line.startswith("* UploadData="):
            header["upload_data"] = line[13:].strip()
        elif ", " in line and " = " in line:
            # Split line into multiple attributes
            for attr in line[2:].split(", "):
                attr, value = attr.split(" = ", 1)
                header[standardize_attribute(attr)] = value.strip()
        elif re.match(r"\*\s[\w\s]+\=", line):
            attr, value = line[2:].split("=", 1)
            header[standardize_attribute(attr)] = value.strip()
        else:
            logger.warning("Unknown line format: %s", line.strip())

    def read_hash_line(line):
        """Read hash(#) line in seabird header"""
        if line.startswith("# name"):
            attrs = re.search(
                r"\# name (?P<id>\d+) = (?P<sbe_variable>[^\s]+)\: (?P<long_name>.*)"
                + r"( \[(?P<units>.*)\](?P<comments>.*))*",
                line,
            ).groupdict()
            header["variables"][int(attrs["id"])] = attrs
        elif line.startswith("# span"):
            span = re.search(r"\# span (?P<id>\d+) = (?P<span>.*)", line)
            values = [
                float(value) if re.search(r".|e", value) else int(value)
                for value in span["span"].split(",")
            ]
            header["variables"][int(span["id"])].update(
                {"value_min": values[0], "value_max": values[1]}
            )
        elif line.startswith("# QA Applied:"):
            # This could be specific to DFO NAFC PCNV format
            header["processing"].append(
                {"module": "QA applied", "message": line.split(":", 1)[1]}
            )
        elif processing_row := is_seabird_processing_stage.match(line):
            if processing_row["parameter"] == "date":
                parameters = dict(
                    zip(["date", "version"], processing_row["value"].split(","))
                )
            else:
                parameters = {processing_row["parameter"]: processing_row["value"]}

            if (
                header["processing"] == []
                or processing_row["module"].lower()
                != header["processing"][-1]["module"].lower()
            ):
                header["processing"].append(
                    {
                        "module": processing_row["module"],
                    }
                )

            header["processing"][-1].update(parameters)
        elif (
            line.startswith("# Using the GSW Toolkit version")
            and header["processing"][-1]["module"] == "DeriveTEOS"
        ):
            # Add GSW toolkit version to the last processing step
            # which should be DeriveTEOS
            header["processing"][-1]["gsw_toolkit_version"] = line[31:].strip()
        elif " = " in line:
            attr, value = line[2:].split("=", 1)
            header[standardize_attribute(attr)] = value.strip()
        elif line.endswith("=\n"):
            header[standardize_attribute(line[2:-3])] = None
        elif ": " in line:
            attr, value = line[2:].split(": ", 1)
            header[standardize_attribute(attr)] = value.strip()
        else:
            logger.warning("Unknown line format: %s", line.strip())

    def parse_xml(xml_section, error_level="ERROR"):
        """Parse XML section"""
        try:
            return xmltodict.parse(f"<temp>{xml_section}</temp>")["temp"]
        except ExpatError:
            logger.log(
                logging.getLevelName(error_level),
                "Failed to parsed Sea-Bird XML",
            )
            return {}

    def _read_next_line():
        line = f.readline()
        header["seabird_header"] += line
        return line

    line = "*"
    header = {
        "instrument_type": "",
        "variables": {},
        "processing": [],
        "calibration": {},
        "history": [],
        "comments": [],
        "seabird_header": "",
    }
    read_next_line = True
    while "*END*" not in line and line.startswith(("*", "#")):
        if read_next_line:
            line = _read_next_line()
        else:
            read_next_line = True

        # Ignore empty lines or last header line
        if line in IGNORED_HEADER_LINES or "*END*" in line:
            continue
        elif re.match(r"(\*|\#)\s*\<", line):
            # Load XML header
            # Retriveve the whole block of XML header
            xml_section = ""
            first_character = line[0]
            while (
                re.match(rf"\{first_character}\s*\<", line)
                or re.search(r"\>\s*$", line)
                or line.startswith("** ")
                or line.startswith("* cast")
                or line in IGNORED_HEADER_LINES
            ):
                if "**" in line:
                    read_comments(line)
                elif line in IGNORED_HEADER_LINES:
                    line = _read_next_line()
                    continue
                elif line.startswith("* cast"):
                    read_asterisk_line(line)
                xml_section += line[1:]
                line = _read_next_line()
            read_next_line = False
            # Add section_name
            if first_character == "*":
                section_name = "data_xml"
            elif first_character == "#":
                section_name = "instrument_xml"
            xml_dictionary = parse_xml(xml_section, error_level=xml_parsing_error_level)
            if section_name in header:
                header[section_name].update(xml_dictionary)
            else:
                header[section_name] = xml_dictionary

            read_next_line = False
        elif line.startswith("** "):
            read_comments(line)
        elif line.startswith("* "):
            read_asterisk_line(line)
        elif line.startswith("# "):
            read_hash_line(line)
        else:
            logger.warning("Unknown line format: %s", line.strip())
    # Remap variables to seabird variables
    variables = {
        attrs["sbe_variable"]: attrs for key, attrs in header["variables"].items()
    }
    header["variables"] = variables

    # Convert time attributes to datetime
    new_attributes = {}
    for key, value in header.items():
        if not isinstance(value, str):
            continue
        time_attr = sbe_time.match(value)
        if time_attr:
            header[key] = datetime.strptime(time_attr["time"], SBE_TIME_FORMAT)
            if "comment" in time_attr.groupdict() and time_attr["comment"] != "":
                new_attributes[key + "_comment"] = time_attr["comment"].strip()
    header.update(new_attributes)

    # btl header row
    if "Bottle" in line:
        var_columns = line[22:-1]
        var_width = 11
        header["bottle_columns"] = ["Bottle", "Date"] + [
            var_columns[index : index + var_width].strip()
            for index in range(0, len(var_columns), var_width)
        ]
        # Read  Position Time line
        line = f.readline()
    return header


def _generate_seabird_cf_history(attrs, drop_processing_attrs=False):
    """Generate CF standard history from Seabird Processing Modules"""
    history = attrs.get("history")
    for step in attrs["processing"]:
        timestamp = pd.to_datetime(step["date"], format=SBE_TIME_FORMAT).isoformat() if "date" in step else "0000-00-00T00:00:00"
        label = (
            "SBEDataProcessing"
            if step["module"] in SBE_DATA_PROCESSING_MODULES
            else "Processing step"
        )
        history.append(f"{timestamp} {label}: {json.dumps(step)}")
    if drop_processing_attrs:
        attrs.pop("processing")
    return attrs


# TODO Integreate IOOS 1.2 attributes to standard seabird module.
seabird_to_bodc = {
    "Temperature": ["TEMPP681", "TEMPP901", "TEMPS601", "TEMPS901", "TEMPPR01"],
    "Temperature, 2": ["TEMPP682", "TEMPP902", "TEMPS602", "TEMPS902", "TEMPPR02"],
    "Pressure, Digiquartz with TC": ["PRESPR01"],
    "Pressure, Strain Gauge": ["PRESPR01"],
    "Conductivity": ["CNDCST01"],
    "Conductivity, 2": ["CNDCST02"],
    "Altimeter": ["AHSFZZ01"],
    "PAR/Logarithmic, Satlantic": ["IRRDUV01"],
    "PAR/Irradiance, Biospherical/Licor": ["IRRDUV01"],
    "Oxygen, SBE 43": ["DOXYZZ01", "OXYOCPVL01"],
    "Oxygen, SBE 43, 2": ["DOXYZZ02", "OXYOCPVL02"],
    "Oxygen Current, Beckman/YSI": ["DOXYZZ01", "OXYOCPVL01"],
    "Oxygen Temperature, Beckman/YSI": ["DOXYZZ01", "OXYOCPVL01"],
    "Optode 4330F - O2 Temp": ["DOXYZZ01", "OXYTPR01"],
    "Optode 4330F - O2 Temperature": ["DOXYZZ01", "OXYTPR01"],
    "Optode 4330F - O2 D-Phase": ["DOXYZZ01", "OXYOCPFR"],
    "Optode 4330F - D Phase": ["DOXYZZ01", "OXYOCPFR"],
    "Optode 4330F - O2 Concentration": ["DOXYZZ01", "OXYOCPFR"],
    "Fluorometer, Seapoint Ultraviolet": ["CDOMZZ01", "CDOMZZ02"],
    "Fluorometer, WET Labs ECO CDOM": ["CDOMZZ01", "CDOMZZ02"],
    "Fluorometer, Chelsea UV Aquatracka": ["CDOMZZ01", "CDOMZZ02"],
    "Fluorometer, Seapoint": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, WET Labs WETstar": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Wetlabs Wetstar": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Wetlab Wetstar": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, WET Labs ECO-AFL/FL": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Seatech/Wetlabs FLF": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Chelsea Aqua": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Chelsea Aqua 3": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Chelsea Minitracka": ["CPHLPR01", "CPHLPR02"],
    "Fluorometer, Seatech/WET Labs FLF": ["CPHLPR01", "CPHLPR02"],
    "Transmissometer, WET Labs C-Star": ["ATTNZS01", "ATTNZR01", "ATTNXXZZ"],
    "Transmissometer, Chelsea/Seatech": ["ATTNZR01", "ATTNXXZZ"],
    "Turbidity Meter, WET Labs, ECO-NTU": ["TURBXX01", "VSCTXX01"],
    "Turbidity Meter, Seapoint": ["TURBXX01", "VSCTXX01"],
    "OBS, Backscatterance (D & A)": ["TURBXX01", "VSCTXX01"],
    "pH": ["PHMASS01", "PHXXZZ01"],
    "OBS, WET Labs, ECO-BB": ["TURBXX01", "VSCTXX01"],
    "OBS, Seapoint Turbidity": ["VSCTXX01", "TURBXX01"],
    "SPAR/Surface Irradiance": ["IRRDSV01"],
    "SPAR, Biospherical/Licor": ["IRRDSV01"],
    "SUNA": [],
    "Dr. Haardt BackScatter Fluorometer": [],
    "User Polynomial": [],
    "User Polynomial, 2": [],
    "User Polynomial, 3": [],
}


def _get_seabird_instrument_from_header(seabird_header: str) -> str:
    """Retrieve main instrument model from Sea-Bird CNV header"""
    instrument = re.findall(
        r"\* (?:Sea\-Bird ){0,1}SBE\s*(?P<sensor>\d+[^\s]*)(?P<extra>.*)",
        seabird_header,
    )
    instrument = [inst for inst, extra in instrument if " = " not in extra]
    if instrument:
        return f"Sea-Bird SBE {''.join(instrument)}"


def _get_sbe_instrument_type(instrument: str) -> str:
    """Map SBE instrument number a type of instrument"""
    if re.match(r"SBE\s*(9|16|19|25|37)", instrument):
        return "CTD"
    logger.warning("Unknown instrument type for %s", instrument)


def _get_seabird_processing_history(seabird_header: str) -> str:
    """
    Retrieve the different rows within a Seabird header associated
    with the sbe data processing tool
    """
    if "# datcnv" in seabird_header:
        sbe_hist = r"\# (" + "|".join(SBE_DATA_PROCESSING_MODULES) + r").*"
        return "\n".join(
            [line for line in seabird_header.split("\n") if re.match(sbe_hist, line)]
        )
    logger.warning("Failed to retrieve Seabird Processing Modules history")


def _generate_binned_attributes(
    ds: xarray.Dataset, seabird_header: str
) -> xarray.Dataset:
    """Retrieve from the Seabird header binned information and
    apply it to the different related attributes and variable attributes."""

    binavg = re.search(
        r"\# binavg_bintype \= (?P<bintype>.*)\n\# binavg_binsize \= (?P<binsize>\d+)\n",
        seabird_header,
    )
    if binavg:
        bintype, binsize = binavg.groups()
    else:
        return ds

    bin_str = f"{binsize} {bintype}"
    ds.attrs["geospatial_vertical_resolution"] = bin_str
    if "decibar" in bintype:
        binvar = "prdM"
    elif "second" in bin_str or "hour" in bin_str:
        binvar = "time"
    elif "meter" in bin_str:
        binvar = "depth"
    elif "scan" in bin_str:
        binvar = "scan"
    else:
        logger.error("Unknown binavg method: %s", bin_str)

    # Add cell method attribute and geospatial_vertical_resolution global attribute
    if "decibar" in bin_str or "meter" in bin_str:
        ds.attrs["geospatial_vertical_resolution"] = bin_str
    elif "second" in bin_str or "hour" in bin_str:
        ds.attrs["time_coverage_resolution"] = pd.Timedelta(bin_str).isoformat()
    for var in ds:
        if (len(ds.dims) == 1 and len(ds[var].dims) == 1) or binvar in ds[var].dims:
            ds[var].attrs["cell_method"] = f"{binvar}: mean (interval: {bin_str})"
    return ds


def _update_attributes_from_seabird_header(
    ds: xarray.Dataset, seabird_header: str, parse_manual_inputs: bool = False
) -> xarray.Dataset:
    """Add Seabird specific attributes parsed from Seabird header into a xarray dataset"""
    # sourcery skip: identity-comprehension, remove-redundant-if
    # Instrument
    ds.attrs["instrument"] = _get_seabird_instrument_from_header(seabird_header)

    # Bin Averaged
    ds = _generate_binned_attributes(ds, seabird_header)

    # Manual inputs
    manual_inputs = re.findall(r"\*\* (?P<key>.*): (?P<value>.*)\n", seabird_header)
    if parse_manual_inputs:
        for key, value in manual_inputs:
            ds.attrs[key.replace(r" ", r"_").lower()] = value
    return ds


def _generate_instruments_variables_from_xml(
    ds: xarray.Dataset, seabird_header: str
) -> xarray.Dataset:
    """Generate IOOS 1.2 standard instrument variables and associated variables
    instrument attribute based on Seabird XML header."""
    # Retrieve Sensors xml section within seabird header
    calibration_xml = re.sub(
        r"\n\#\s",
        r"\n",
        re.search(r"\<Sensors .+\<\/Sensors\>", seabird_header, re.DOTALL)[0],
    )

    # Read XML and commented lines, drop encoding line
    try:
        sensors = xmltodict.parse(calibration_xml)["Sensors"]["sensor"]
    except ExpatError:
        logger.error("Failed to parsed Sea-Bird Instrument Calibration XML")
        return ds, {}

    sensors_comments = re.findall(
        r"\s*\<!--\s*(Frequency \d+|A/D voltage \d+|.* voltage|Count|Serial RS-232){1}, (.*)-->\n",
        calibration_xml,
    )
    # Consider only channels with sensor mounted
    sensors = [sensor for sensor in sensors if len(sensor) > 1]
    sensors_comments = [
        (con, name)
        for con, name in sensors_comments
        if not name.startswith(("Free", "Unavailable"))
    ]

    # Make sure that the sensor count match the sensor_comments count
    if len(sensors_comments) != len(sensors):
        logger.error("Failed to detect same count of sensors and sensors_comments")
        return ds, {}

    # Split each sensor calibrations to a dictionary
    sensors_map = {}
    for sensor, sensor_comment in zip(sensors, sensors_comments):
        sensor_key = list(sensor.keys())[1].strip()
        attrs = sensor[sensor_key]
        channel_type, description = sensor_comment

        # Define senor variable name
        if "UserPolynomial" in sensor_key and attrs.get("SensorName"):
            sensor_name = attrs.pop("SensorName").strip()
            sensor_var_name = re.sub(r"[^\d\w]+", "_", sensor_name)
        else:
            sensor_var_name = sensor_key
            sensor_name = description.strip()

        if "Oxygen" in sensor_name:
            subsensors = re.search(r"Current|Temp|Phase|Concentration", description)
            if subsensors:
                sensor_var_name += "_" + subsensors[0]

        # Add trailing number if present
        if re.search(r", \d+", sensor_name):
            sensor_number = int(re.search(r", (\d+)", sensor_name)[1])
            sensor_var_name += f"_{sensor_number}"
        else:
            sensor_number = 1

        if sensor_var_name in ds:
            logger.warning("Duplicated instrument variable %s", sensor_var_name)

        # Try fit IOOS 1.2 which request to add a instrument variable for each
        # instruments and link this variable to data variable by using the instrument attribute
        # https://ioos.github.io/ioos-metadata/ioos-metadata-profile-v1-2.html#instrument
        ds[sensor_var_name] = json.dumps(attrs)
        ds[sensor_var_name].attrs = {
            "calibration_date": convert_datetime_str(
                attrs.pop("CalibrationDate"),
                errors="ignore",
            ),  # IOOS 1.2, NCEI 2.0
            "component": f"{sensor_var_name}_sn{attrs['SerialNumber']}",  # IOOS 1.2
            "discriminant": str(sensor_number),  # IOOS 1.2
            "make_model": sensor_name,  # IOOS 1.2, NCEI 2.0
            "channel": sensor["@Channel"],
            "channel_type": channel_type,
            "sbe_sensor_id": int(attrs.pop("@SensorID")),
            "serial_number": attrs.pop("SerialNumber"),  # NCEI 2.0
            "calibration": json.dumps(attrs),
        }
        sensors_map[sensor_name] = sensor_name

    return ds, sensors_map


def _generate_instruments_variables_from_sensor(
    dataset: xarray.Dataset, seabird_header: str
) -> xarray.Dataset:
    """Parse older Seabird Header sensor information and generate instrument variables"""
    sensors = re.findall(r"\# sensor (?P<id>\d+) = (?P<text>.*)\n", seabird_header)
    for index, sensor in sensors:
        if "Voltage" in sensor:
            sensor_items = sensor.split(",", 1)
            attrs = {
                "channel": sensor_items[0],
                "sensor_description": sensor_items[0].replace("Voltage", "").strip()
                + sensor_items[1],
            }
        else:
            attrs_dict = re.search(
                r"(?P<channel>Frequency\s+\d+|Stored Volt\s+\d+|Extrnl Volt  \d+|Pressure Number\,)\s+"
                + r"(?P<sensor_description>.*)",
                sensor,
            )
            if attrs_dict is None:
                logger.error("Failed to read sensor item: %s", sensor)
                continue
            attrs = attrs_dict.groupdict()
        sensor_code = f"sensor_{index}"
        dataset[sensor_code] = sensor
        dataset[sensor_code].attrs = attrs
    return dataset


def _add_seabird_instruments(
    ds: xarray.Dataset, seabird_header: str, match_by: str = "long_name"
) -> xarray.Dataset:
    """
    Extract seabird sensor information and generate instrument variables which
    follow the IOOS 1.2 convention
    """
    # Retrieve sensors information
    if "# <Sensors count" in seabird_header:
        ds, sensors_map = _generate_instruments_variables_from_xml(ds, seabird_header)
    elif "# sensor" in seabird_header:
        ds = _generate_instruments_variables_from_sensor(ds, seabird_header)
        logger.info("Unable to map old seabird sensor header to appropriate variables")
        return ds
    else:
        # If no calibration detected give a warning and return dataset
        logger.info("No Seabird sensors information was detected")
        return ds

    # Match instrument variables to their associated variables
    for name, sensor_variable in sensors_map.items():
        if match_by == "sdn_parameter_urn":
            if name not in seabird_to_bodc:
                logger.warning("Missing Seabird to BODC mapping of: %s", name)
                continue
            values = [f"SDN:P01::{item}" for item in seabird_to_bodc[name]]
        else:
            values = [name]

        has_matched = False
        for value in values:
            matched_variables = ds.filter_by_attrs(**{match_by: value})

            # Some variables are not necessearily BODC specifc
            # we'll try to match them based on the long_name
            if (
                len(matched_variables) > 1
                and match_by == "sdn_parameter_urn"
                and ("Fluorometer" in name or "Turbidity" in name)
            ):
                # Find the closest match based on the file name
                var_longname = difflib.get_close_matches(
                    name,
                    [
                        matched_variables[var].attrs["long_name"]
                        for var in matched_variables
                    ],
                )
                matched_variables = matched_variables[
                    [
                        var
                        for var in matched_variables
                        if ds[var].attrs["long_name"] in var_longname
                    ]
                ]

                # If there's still multiple matches give a warning
                if len(matched_variables) > 1:
                    logger.warning(
                        "Unable to link multiple %s instruments via sdn_parameter_urn attribute.",
                        name,
                    )

            for var in matched_variables:
                if ds[var].attrs.get("instrument"):
                    ds[var].attrs["instrument"] += "," + sensor_variable
                else:
                    ds[var].attrs["instrument"] = sensor_variable
                has_matched = True
        if not has_matched:
            logger.info("Failed to match instrument %s to any variables.", name)

    return ds
