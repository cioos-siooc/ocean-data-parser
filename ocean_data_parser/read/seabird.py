"""
This module regroup a set of tools used to convert the Seabird Electronic different
formats to a CF compliant xarray format.
"""

import argparse
import json
import logging
import os
import re
from datetime import datetime

import pandas as pd
import xarray as xr
import xmltodict

from .utils import standardize_dateset

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

# Read vocabulary file
with open(reference_vocabulary_path, encoding="UTF-8") as vocabulary_file:
    seabird_variable_attributes = json.load(vocabulary_file)

    # Make it non case sensitive by lowering all keys
    seabird_variable_attributes = {
        key.lower(): attrs for key, attrs in seabird_variable_attributes.items()
    }


def _convert_to_netcdf_var_name(var_name):
    """Convert seabird variable name to a netcdf compatible format."""
    return var_name.replace("/", "Per")


def _add_seabird_vocabulary(variable_attributes):
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


def cnv(file_path, encoding="UTF-8", kwargs_read_csv=None):
    """Import Seabird cnv format as an xarray dataset."""
    if kwargs_read_csv is None:
        kwargs_read_csv = {}
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
            **kwargs_read_csv,
        )

    header = _generate_seabird_cf_history(header)

    ds = _convert_sbe_dataframe_to_dataset(df, header)
    return standardize_dateset(ds)


def btl(file_path, encoding="UTF-8", kwargs_read_fwf=None):
    """Import Seabird btl format as an xarray dataset."""
    if kwargs_read_fwf is None:
        kwargs_read_fwf = {}

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
            **kwargs_read_fwf,
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

    return standardize_dateset(ds)


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
                re.match(f"\{first_character}\s*\<", line)
                or re.match(f"^\{first_character}\s*$", line)
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


# TODO add method to include instrument variables compatible iwth IOOS 1.2 to file.

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse Seabird Instruments File Formats"
    )
    parser.add_argument(dest="input", help="file inptut (btl or cnv", default=None)

    args = parser.parse_args()
    if args.input:
        if args.input.endswith("btl"):
            btl(args.input)
        elif args.input.endswith("cnv"):
            cnv(args.input)
