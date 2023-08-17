"""
# Seabird Scientific
<https://www.seabird.com>

"""

import difflib
import json
import logging
import os
import re
from datetime import datetime

import pandas as pd
import xarray
import xmltodict
from pyexpat import ExpatError

from ocean_data_parser.parsers.utils import convert_datetime_str, standardize_dataset
from ocean_data_parser.vocabularies.load import seabird_vocabulary

SBE_TIME_FORMAT = "%b %d %Y %H:%M:%S"  # Jun 23 2016 13:51:30
var_dtypes = {
    "date": str,
    "bottle": str,
    "Flag": int,
    "flag": int,
    "stats": str,
    "scan": int,
}
sbe_time = re.compile(
    r"(?P<time>\w\w\w\s+\d{1,2}\s+\d{1,4}\s+\d\d\:\d\d\:\d\d)(?P<comment>.*)"
)
logger = logging.getLogger(__name__)

reference_vocabulary_path = os.path.join(
    os.path.dirname(__file__), "vocabularies", "seabird_variable_attributes.json"
)
seabird_variable_attributes = seabird_vocabulary()


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
    file_path: str, encoding: str = "UTF-8"
) -> xarray.Dataset:
    """Parse Seabird CNV format

    Args:
        file_path (str): file path
        encoding (str, optional): encoding to use. Defaults to "UTF-8".

    Returns:
        xarray.Dataset: Dataset
    """    
    """Import Seabird cnv format as an xarray dataset."""

    with open(file_path, encoding=encoding) as f:
        header = _parse_seabird_file_header(f)
        header["variables"] = _add_seabird_vocabulary(header["variables"])
        df = pd.read_csv(
            f,
            delimiter=r"\s+",
            names=header["variables"].keys(),
            dtype={
                var: var_dtypes.get(var, float) for var in header["variables"].keys()
            },
            na_values=["-1.#IO", "-9.99E-29"],
        )

    header = _generate_seabird_cf_history(header)

    ds = _convert_sbe_dataframe_to_dataset(df, header)
    return standardize_dataset(ds)


def btl(
    file_path: str, encoding: str = "UTF-8"
) -> xarray.Dataset:
    """Parse Seabird BTL format

    Args:
        file_path (str): file path
        encoding (str, optional): Encoding to use. Defaults to "UTF-8".

    Returns:
        xarray.Dataset: Dataset
    """

    with open(file_path, encoding=encoding) as f:
        header = _parse_seabird_file_header(f)

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
    n_scan_per_bottle = int(header["datcnv_scans_per_bottle"])
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


def _parse_seabird_file_header(f):
    """Parsed seabird file headers"""

    def unknown_line(line):
        if line in ("* S>\n"):
            return
        header["history"] += [re.sub(r"\*\s|\n", "", line)]
        if line.startswith(("* advance", "* delete")) or "added to scan" in line:
            return
        logger.warning("Unknown line format: %s", line)

    def standardize_attribute(attribute):
        return re.sub(r" |\|\)|\/", "_", attribute.strip()).lower()

    def read_comments(line):
        if re.match(r"\*\* .*(\:|\=).*", line):
            result = re.match(r"\*\* (?P<key>.*)(\:|\=)(?P<value>.*)", line)
            header[result["key"].strip()] = result["value"].strip()
        else:
            header["comments"] += [line[2:]]

    def read_asterisk_line(line):
        if " = " in line:
            attr, value = line[2:].split("=", 1)
            header[standardize_attribute(attr)] = value.strip()
        elif line.startswith((r"* Sea-Bird", r"* SBE ")):
            instrument_type = re.search(
                r"\* Sea-Bird (.*) Data File\:|\* SBE (.*)", line
            ).groups()
            header["instrument_type"] += "".join(
                [item for item in instrument_type if item]
            )
        elif re.match(r"\* Software version .*", line, re.IGNORECASE):
            header["software_version"] = re.search(
                r"\* Software version (.*)", line, re.IGNORECASE
            )[1]
        else:
            unknown_line(line)

    def read_number_line(line):
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
        elif " = " in line:
            attr, value = line[2:].split("=", 1)
            header[standardize_attribute(attr)] = value.strip()
        else:
            unknown_line(line)

    line = "*"
    header = {}
    header["variables"] = {}
    header["instrument_type"] = ""
    header["history"] = []
    read_next_line = True
    while "*END*" not in line and line.startswith(("*", "#")):
        if read_next_line:
            line = f.readline()
        else:
            read_next_line = True

        # Ignore empty lines or last header line
        if (
            re.match(r"^\*\s*$", line)
            or "*END*" in line
            or re.match(r"^\s*Bottle .*", line)
        ):
            continue

        if re.match(r"(\*|\#)\s*\<", line):
            # Load XML header
            # Retriveve the whole block of XML header
            xml_section = ""
            first_character = line[0]
            while (
                re.match(rf"\{first_character}\s*\<", line)
                or re.match(rf"^\{first_character}\s*$", line)
                or line.startswith("** ")
                or line.startswith("* cast")
                or re.search(r"\>\s*$", line)
            ):
                if "**" in line:
                    read_comments(line)
                xml_section += line[1:]
                line = f.readline()
            read_next_line = False
            # Add section_name
            if first_character == "*":
                section_name = "data_xml"
            elif first_character == "#":
                section_name = "instrument_xml"
            xml_dictionary = xmltodict.parse(f"<temp>{xml_section}</temp>")["temp"]
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
            read_number_line(line)
        else:
            unknown_line(line)
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
    #    header = {key: datetime.strptime(value,SBE_TIME_FORMAT)
    # if sbe_time.match(value) else value for key, value in header.items()}

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
    """Generate CF standard history from Seabird parsed attributes"""
    sbe_processing_steps = ("datcnv", "bottlesum")
    history = []
    for step in sbe_processing_steps:
        step_attrs = {
            key.replace(step + "_", ""): value
            for key, value in attrs.items()
            if key.startswith(step)
        }
        if not step_attrs:
            continue

        date_line = step_attrs.pop("date")
        if isinstance(date_line, datetime):
            iso_date_str = date_line.isoformat()
            extra = attrs.pop(step + "_comment") if f"{step}_comment" in attrs else None
        else:
            date_str, extra = date_line.split(",", 1)
            iso_date_str = pd.to_datetime(date_str).isoformat()
        if extra:
            extra = re.search(
                r"^\s(?P<software_version>[\d\.]+)\s*(?P<date_extra>.*)", extra
            )
            step_attrs.update(extra.groupdict())
        history += [f"{iso_date_str} - {step_attrs}"]
    # Sort history by date
    attrs["history"] = "\n".join(attrs.get("history", []) + sorted(history))

    # Drop processing attributes
    if drop_processing_attrs:
        drop_keys = [
            key for key in attrs.keys() if key.startswith(sbe_processing_steps)
        ]
        for key in drop_keys:
            attrs.pop(key)
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

sbe_data_processing_modules = [
    "datcnv",
    "filter",
    "align",
    "celltm",
    "loopedit",
    "derive",
    "Derive",
    "binavg",
    "split",
    "strip",
    "section",
    "wild",
    "window",
]


def get_seabird_instrument_from_header(seabird_header: str) -> str:
    """Retrieve main instrument model from Sea-Bird CNV header"""
    instrument = re.findall(
        r"\* (?:Sea\-Bird ){0,1}SBE\s*(?P<sensor>\d+[^\s]*)(?P<extra>.*)",
        seabird_header,
    )
    instrument = [inst for inst, extra in instrument if " = " not in extra]
    if instrument:
        return f"Sea-Bird SBE {''.join(instrument)}"


def get_sbe_instrument_type(instrument: str) -> str:
    """Map SBE instrument number a type of instrument"""
    if re.match(r"SBE\s*(9|16|19|25|37)", instrument):
        return "CTD"
    logger.warning("Unknown instrument type for %s", instrument)


def get_seabird_processing_history(seabird_header: str) -> str:
    """
    Retrieve the different rows within a Seabird header associated
    with the sbe data processing tool
    """
    if "# datcnv" in seabird_header:
        sbe_hist = r"\# (" + "|".join(sbe_data_processing_modules) + r").*"
        return "\n".join(
            [line for line in seabird_header.split("\n") if re.match(sbe_hist, line)]
        )
    logger.warning("Failed to retrieve Seabird Processing Modules history")


def generate_binned_attributes(
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


def update_attributes_from_seabird_header(
    ds: xarray.Dataset, seabird_header: str, parse_manual_inputs: bool = False
) -> xarray.Dataset:
    """Add Seabird specific attributes parsed from Seabird header into a xarray dataset"""
    # sourcery skip: identity-comprehension, remove-redundant-if
    # Instrument
    ds.attrs["instrument"] = get_seabird_instrument_from_header(seabird_header)

    # Bin Averaged
    ds = generate_binned_attributes(ds, seabird_header)

    # Manual inputs
    manual_inputs = re.findall(r"\*\* (?P<key>.*): (?P<value>.*)\n", seabird_header)
    if parse_manual_inputs:
        for key, value in manual_inputs:
            ds.attrs[key.replace(r" ", r"_").lower()] = value

    return ds


def generate_instruments_variables_from_xml(
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
        r"\s*\<!--\s*(Frequency \d+|A/D voltage \d+|.* voltage|Count){1}, (.*)-->\n",
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
        channel, description = sensor_comment

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
            "channel": channel,
            "sbe_sensor_id": int(attrs.pop("@SensorID")),
            "serial_number": attrs.pop("SerialNumber"),  # NCEI 2.0
            "calibration": json.dumps(attrs),
        }
        sensors_map[sensor_name] = sensor_name

    return ds, sensors_map


def generate_instruments_variables_from_sensor(
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


def add_seabird_instruments(
    ds: xarray.Dataset, seabird_header: str, match_by: str = "long_name"
) -> xarray.Dataset:
    """
    Extract seabird sensor information and generate instrument variables which
    follow the IOOS 1.2 convention
    """
    # Retrieve sensors information
    if "# <Sensors count" in seabird_header:
        ds, sensors_map = generate_instruments_variables_from_xml(ds, seabird_header)
    elif "# sensor" in seabird_header:
        ds = generate_instruments_variables_from_sensor(ds, seabird_header)
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
