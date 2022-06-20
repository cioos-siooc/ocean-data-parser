import pandas as pd
import re
import logging
import xarray as xr
import xmltodict
import json
import os

import argparse

SBE_TIME_FORMAT = "%B %d %Y %H:%m:%s"  # Jun 23 2016 13:51:30
logger = logging.getLogger(__name__)

reference_vocabulary_path = os.path.join(
    os.path.dirname(__file__), "vocabularies", "seabird_variable_attributes.json"
)
with open(reference_vocabulary_path) as f:
    seabird_variable_attributes = json.load(f)


def convert_to_netcdf_var_name(var_name):
    return var_name.replace("/", "Per")


def add_seabird_vocabulary(variable_attributes):
    for var in variable_attributes.keys():
        if var in seabird_variable_attributes:
            variable_attributes[var].update(seabird_variable_attributes[var])
        else:
            logger.warning(f"Variable {var} is missing from vocabulary dictionary")
    return variable_attributes


def cnv(file_path, output="xarray"):
    with open(file_path) as f:
        header = parse_seabird_file_header(f)
        header["variables"] = add_seabird_vocabulary(header["variables"])
        df = pd.read_csv(f, delimiter="\s+", names=header["variables"].keys())

    header = generate_seabird_cf_history(header)

    if output == "dataframe":
        return df, header
    return convert_sbe_dataframe_to_dataset(df, header)


def btl(file_path, output="xarray"):
    with open(file_path) as f:
        header = parse_seabird_file_header(f)
        if header["variables"]:
            header["variables"] = add_seabird_vocabulary(header["variables"])
        else:
            # Retrieve variables from bottle header and lowwer the first letter of each variable
            header["variables"] = add_seabird_vocabulary(
                {var[0].lower() + var[1:]: {} for var in header["bottle_columns"]}
            )
        # parse column header with fix width
        variable_list = list(header["variables"].keys())
        variable_list += ["stats"]
        df = pd.read_fwf(
            f,
            widths=[10, 12] + [11] * (len(header["bottle_columns"]) - 1),
            names=variable_list,
        )

    # Split statistical data info separate dateframes
    df["bottle"] = df["bottle"].ffill().astype(int)
    df["stats"] = df["stats"].str.extract("\((.*)\)")
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
    header = generate_seabird_cf_history(header)
    if output == "dataframe":
        return df, header

    # Convert to xarray
    ds = convert_sbe_dataframe_to_dataset(df, header)

    # Add attributes to std variables and add cell_method
    for var in ds:
        var_std = var + "_sdev"
        if var_std in ds:
            ds[var_std].attrs = ds[var].attrs
            ds[var_std].attrs[
                "cell_method"
            ] = f"scan: standard_deviation (previous {n_scan_per_bottle} scans)"
            # TODO confirm that seabird uses the previous records from this timestamp
        if var not in ["time", "bottle"]:
            ds[var].attrs[
                "cell_method"
            ] = f"scan: mean (previous {n_scan_per_bottle} scans)"

    return ds


def convert_sbe_dataframe_to_dataset(df, header):
    # Convert column names to netcdf compatible format
    df.columns = [convert_to_netcdf_var_name(var) for var in df.columns]
    header["variables"] = {
        convert_to_netcdf_var_name(var): attrs
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


def parse_seabird_file_header(f):
    def unknown_line(line):
        if line in ("* S>\n"):
            return
        header["history"] += [re.sub("\*\s|\n", "", line)]
        logger.warning(f"Unknown line format: {line}")

    def standardize_attribute(attribute):
        return attribute.strip().replace(" ", "_").lower()

    def read_comments(line):
        if re.match("\*\* .*(\:|\=).*", line):
            r = re.match("\*\* (?P<key>.*)(\:|\=)(?P<value>.*)", line)
            header[r["key"].strip()] = r["value"].strip()
        else:
            header["comments"] += [line[2:]]

    def read_asterisk_line(line):
        if " = " in line:
            attr, value = line[2:].split("=", 1)
            header[standardize_attribute(attr)] = value.strip()
        elif line.startswith(("* Sea-Bird", "* SBE ")):
            instrument_type = re.search(
                "\* Sea-Bird (.*) Data File\:|\* SBE (.*)", line
            ).groups()
            header["instrument_type"] += "".join(
                [item for item in instrument_type if item]
            )
        elif re.match(r"\* Software version .*", line, re.IGNORECASE):
            header["software_version"] = re.search(
                "\* Software version (.*)", line, re.IGNORECASE
            )[1]
        else:
            unknown_line(line)

    def read_number_line(line):
        if line.startswith("# name"):
            attrs = re.search(
                "\# name (?P<id>\d+) = (?P<sbe_variable>[^\s]+)\: (?P<long_name>.*)( \[(?P<units>.*)\](?P<comments>.*))*",
                line,
            ).groupdict()
            header["variables"][int(attrs["id"])] = attrs
        elif line.startswith("# span"):
            span = re.search("\# span (?P<id>\d+) = (?P<span>.*)", line)
            values = [
                float(value) if re.search(".|e", value) else int(value)
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
            re.match("^\*\s*$", line)
            or "*END*" in line
            or re.match("^\s*Bottle .*", line)
        ):
            continue

        if re.match("(\*|\#)\s*\<", line):
            # Load XML header
            # Retriveve the whole block of XML header
            xml_section = ""
            first_character = line[0]
            while (
                re.match(f"\{first_character}\s*\<", line)
                or re.match(f"^\{first_character}\s*$", line)
                or line.startswith("** ")
                or line.startswith("* cast")
                or re.search("\>\s*$", line)
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


def generate_seabird_cf_history(attrs, drop_processing_attrs=False):
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
        date_str, extra = date_line.split(",", 1)
        iso_date_str = pd.to_datetime(date_str).isoformat()
        if extra:
            extra = re.search(
                "^\s(?P<software_version>[\d\.]+)\s*(?P<date_extra>.*)", extra
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
