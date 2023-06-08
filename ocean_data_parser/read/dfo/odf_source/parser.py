"""
odf_parser is a module that regroup a different set of tools used to
parse the ODF format which is use, maintain and developped
by the DFO offices BIO and MLI.
"""

import logging
import re
from datetime import datetime, timezone

import gsw
import numpy as np
import pandas as pd
import xarray as xr

no_file_logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(no_file_logger, {"file": None})

# Dictionary with the mapping of the odf types to python types
odf_dtypes = {
    "DOUB": "float64",
    "SING": "float32",
    "DOUBLE": "float64",
    "SYTM": str,
    "INTE": "int32",
    "CHAR": str,
    "QQQQ": "int32",
}

vocabulary_attribute_list = [
    "long_name",
    "units",
    "instrument",
    "scale",
    "standard_name",
    "sdn_parameter_urn",
    "sdn_parameter_name",
    "sdn_uom_urn",
    "sdn_uom_name",
    "coverage_content_type",
    "ioos_category",
    "comments",
]

# Commonly date place holder used within the ODF file
FLAG_LONG_NAME_PREFIX = "Quality_Flag: "
ORIGINAL_PREFIX_VAR_ATTRIBUTE = "original_"


class GF3Code:
    """
    ODF GF3 Class split terms in their different components and
    standardize the convention (CODE_XX).
    """

    def __init__(self, code):
        self.code = re.search(r"^[^_]*", code)[0]
        index = re.search(r"\d+$", code)
        self.index = int(index[0]) if index else 1
        self.name = f"{self.code}_{self.index:02}" if index else self.code


def _convert_odf_time(time_string):
    """Convert ODF timestamps to a datetime object"""
    if time_string == "17-NOV-1858 00:00:00.00":
        return pd.NaT

    delta_time = (
        pd.Timedelta("1min") if re.search(r":60.0+", time_string) else pd.Timedelta(0)
    )
    if delta_time.total_seconds() > 0:
        time_string = re.sub(r":60.0+", ":00.00", time_string)

    # Detect time format
    if re.match(r"\d+-\w\w\w-\d\d\d\d\s*\d+\:\d\d\:\d\d\.\d+", time_string):
        time_format = r"%d-%b-%Y %H:%M:%S.%f"
    elif re.match(r"\d\d-\w\w\w-\d\d\d\d\s*\d\d\:\d\d\:\d\d", time_string):
        time_format = r"%d-%b-%Y %H:%M:%S"
    else:
        logger.warning("Unknown time format: %s", time_string)
        time_format = "infer"

    # Conver to datetime object
    time = (
        pd.to_datetime(time_string, format=time_format, utc=True, errors="coerce")
        + delta_time
    )
    if time is pd.NaT and time_string:
        logger.warning(
            "Failed to parse the timestamp=%s, it will be replaced by NaT", time_string
        )

    # Check if time is valid
    if time < pd.to_datetime("1990-Jan-01", format="%Y-%b-%d", utc=True):
        logger.warning(
            "Time stamp '%s' = %s is before 1900-01-01 which is very suspicious",
            time_string,
            time,
        )
    return time


def history_input(comment, date=datetime.now(timezone.utc)):
    """Genereate a CF standard history line: Timstamp comment"""
    return f"{date.strftime('%Y-%m-%dT%H:%M:%SZ')} {comment}\n"


def update_variable_index(varname, index):
    """Standardize variables trailing number to two digits"""
    if varname.endswith(("XX", "01")):
        return f"{varname[:-2]}{index:02}"
    elif varname.endswith(("X", "1")):
        return f"{varname[:-1]}{index:01}"
    else:
        return varname


def read(filename, encoding_format="Windows-1252"):
    """
    Read_odf
    Read_odf parse the odf format used by some DFO organisation to python list of
    dictionary format and pandas dataframe. Once converted, the output can easily
    be converted to netcdf format.

    Steps applied:
        1. Read line by line an ODF header and distribute each lines in a list of
           list and dictionaries.
            a. Lines associated with a character at the beginning are considered a section.
            b. Lines starting white spaces are considered items in preceding section.
            c. Repeated sections are grouped as a list
            d. Each section items are grouped as a dictionary
            e. dictionary items are converted to datetime (deactivated), string, integer or
                float format.
        2. Read the data  following the header with Pandas.read_csv() method
            a. Use defined separator  to distinguish columns (default multiple white spaces).
            b. Convert each column of the pandas data frame to the matching format specified in
            the TYPE attribute of the ODF associated PARAMETER_HEADER

    read_odf is a simple tool that  parse the header metadata and data from an DFO
    ODF file to a list of dictionaries.
    :param filename: ODF file to read
    :param encoding_format: odf encoding format
     start of the data.
    :return:
    """

    def _cast_value(value: str):
        """Attemp to cast value in line "key=value" of ODF header:
        - integer
        - float
        - date
        - else string
        """
        # Drop quotes and comma
        value = re.sub(r"^'|,$|',$|'$", "", value)

        # Convert numerical values to float and integers
        if "LATITUDE" in key and value == "-99.9":
            return None
        elif "LONGITUDE" in key and value == "-999.9":
            return None
        elif re.match(r"[-+]{0,1}\d+\.\d+$", value):
            return float(value)
        elif re.match(r"[-+]{0,1}\d*\.\d+[ED][+-]\d+$", value):
            return float(value.replace("D", "E"))
        elif re.match(r"[-+]{0,1}\d+$", value):
            return int(value)
        elif value == "17-NOV-1858 00:00:00.00":
            return pd.NaT
        elif re.match(r"^\d{1,2}-\w\w\w\-\d\d\d\d\s*\d\d:\d\d:\d\d\.*\d*$", value):
            try:
                return _convert_odf_time(value)
            except pd.errors.ParserError:
                logger.warning(
                    "Failed to read date '%s' in line: %s",
                    value,
                    line,
                    exc_info=True,
                )
                return value
        # Empty lines
        elif re.match(r"^\s*$", value):
            return None
        # if do not match any conditions return unchanged
        return value

    metadata = {}  # Start with an empty dictionary
    with open(filename, encoding=encoding_format) as f:
        line = ""
        original_header = []
        # Read header one line at the time
        for line in f:
            line = line.replace("\n", "")
            original_header.append(line)
            # Read header only
            if "-- DATA --" in line:
                break

            # Sections
            if re.match(r"\s{0,1}[A-Z_]+,{0,1}\s*", line):
                section = re.search(r"\s*([A-Z_]*)", line)[1]
                if section not in metadata:
                    metadata[section] = [{}]
                else:
                    metadata[section].append({})
                continue

            elif "=" in line:  # Something=This
                key, value = [item.strip() for item in line.split("=", 1)]
            else:
                logger.error("Unrecognizable line format: %s", line)
                continue

            # Parse metadata row
            key = key.strip().replace(" ", "_")
            value = _cast_value(value)

            # Add to the metadata as a dictionary
            if key in metadata[section][-1]:
                if not isinstance(metadata[section][-1][key], list):
                    metadata[section][-1][key] = [metadata[section][-1][key]]
                metadata[section][-1][key].append(value)
            else:
                metadata[section][-1][key] = value

        # Simplify the single sections to a dictionary
        temp_metadata = metadata.copy()
        for section, items in metadata.items():
            if (
                len(items) == 1
                and isinstance(items[0], dict)
                and section
                not in ["HISTORY_HEADER", "PARAMETER_HEADER", "QUALITY_HEADER"]
            ):
                temp_metadata[section] = temp_metadata[section][0]
        metadata = temp_metadata

        # Add original header in text format to the dictionary
        metadata["original_header"] = original_header

        # READ PARAMETER_HEADER
        # Define first the variable name and attributes and the type.
        metadata["variable_attributes"] = {}
        time_columns = []
        # Variable names and related attributes
        for att in metadata["PARAMETER_HEADER"]:
            # Generate variable name
            var_name = GF3Code(att.get("CODE") or att.get("WMO_CODE")).name
            if var_name is None:
                raise RuntimeError("Unrecognizable ODF variable attributes")

            # Generate variable attributes
            metadata["variable_attributes"][var_name] = {
                "long_name": att.get("NAME"),
                "units": att.get("UNITS", "").replace("**", "^"),
                "legacy_gf3_code": var_name,
                "null_value": att["NULL_VALUE"],
                "resolution": (
                    10 ** -att["PRINT_DECIMAL_PLACES"]
                    if not att["PRINT_DECIMAL_PLACES"] == -99
                    else None
                ),
            }
            # Time type column add to time variables to parse by pd.read_csv()
            if var_name.startswith("SYTM") or att["TYPE"] == "SYTM":
                time_columns += [var_name]

        # Read Data with Pandas
        data_raw = pd.read_csv(
            f,
            delimiter=r"\s+",
            quotechar="'",
            header=None,
            names=metadata["variable_attributes"].keys(),
            na_values={
                key: att.pop("null_value")
                for key, att in metadata["variable_attributes"].items()
            },
            date_parser=_convert_odf_time,
            parse_dates=time_columns,
            encoding=encoding_format,
        )

    # Review N variables
    if len(data_raw.columns) != len(metadata["PARAMETER_HEADER"]):
        raise RuntimeError(
            f"{len(data_raw.columns)}/{len(metadata['PARAMETER_HEADER'])} variables were detected"
        )

    return metadata, data_raw


def odf_flag_variables(dataset, flag_convention=None):
    """
    odf_flag_variables handle the different conventions used within the ODF files
    over the years and map them to the CF standards.
    """

    def _add_ancillary(ancillary, variable):
        dataset[variable].attrs[
            "ancillary_variables"
        ] = f"{dataset[variable].attrs.get('ancillary_variables','')} {ancillary}".strip()
        return dataset[variable]

    # Loop through each variables and detect flag variables
    variables = list(dataset.variables)

    # Rename QQQQ flag convention
    qqqq_flags = {
        var: f"Q{variables[id-1]}"
        for id, var in enumerate(variables)
        if var.startswith("QQQQ")
    }
    if qqqq_flags:
        dataset = dataset.rename(qqqq_flags)
        dataset.attrs["history"] += history_input(
            f"Rename QQQQ flags to QXXXX convention: {qqqq_flags}",
        )

    # Add ancillary_variable attribute
    for variable in dataset.variables:
        if variable.startswith(("QCFF", "FFFF")):
            # add QCFF and FFFF as ancillary variables
            # to all non flag variables
            for var in dataset.variables:
                if not var.startswith("Q"):
                    _add_ancillary(variable, var)
        elif variable.startswith("Q") and variable[1:] in dataset:
            dataset[variable] = _add_ancillary(variable, variable[1:])
            dataset[variable].attrs[
                "long_name"
            ] = f"Quality_Flag: {dataset[variable[1:]].attrs['long_name']}"
        else:
            # ignore normal variables
            continue

        # Add flag convention attributes
        dataset[variable].attrs.update(
            flag_convention.get(variable, flag_convention["default"])
        )
        dataset[variable] = dataset[variable].astype(int)
        dataset[variable].attrs.pop("units", None)

    # TODO handle renamed variables associated flags
    return dataset


def get_vocabulary_attributes(ds, organizations=None, vocabulary=None):
    """
    This method is use to retrieve from an ODF variable code, units and units,
    matching vocabulary terms available.
    """

    def _add_reference_scale():
        """Retrieve scale information from  either units or long_name"""
        scales = {
            "IPTS-48": r"IPTS\-48",
            "IPTS-68": r"IPTS\-68|ITS\-68",
            "ITS-90": r"ITS\-90|TE90",
            "PSS-78": r"PSS\-78|practical.*salinity|psal",
        }
        for scale, scale_search in scales.items():
            if re.search(
                scale_search, ds[var].attrs.get("units"), re.IGNORECASE
            ) or re.search(scale_search, ds[var].attrs.get("long_name"), re.IGNORECASE):
                ds[var].attrs["scale"] = scale

    def _review_term(term, accepted_terms, regexp=False, search_flag=None):
        """
        Simple tool to compare "|" separated units in the Vocabulary expected unit list.
        - First unit if any is matching.
        - True if empty or expected to be empty
        - unknown if unit exists but the "accepted_units" input is empty.
        - False if not matching units
        """
        return bool(
            accepted_terms is None
            or any(
                item in accepted_terms.split("|")
                for item in ("none", "dimensionless", term)
            )
            or (regexp and re.search(accepted_terms, term, search_flag))
        )

    def _get_matching_vocabularies():
        """Match variable to vocabulary by:
        - vocabulary
        - gf3 code
        - units
        - scale
        - long_name
        - global instrument_type instrument_model
        """
        # Among these matching terms find matching ones
        match_vocabulary = vocabulary["Vocabulary"].isin(organizations)
        match_code = (
            vocabulary["name"] == ds[var].attrs["legacy_gf3_code"].split("_")[0]
        )
        match_units = vocabulary["accepted_units"].apply(
            lambda x: _review_term(ds[var].attrs.get("units"), x)
        )
        match_scale = vocabulary["accepted_scale"].apply(
            lambda x: _review_term(ds[var].attrs.get("scale"), x)
        )
        match_instrument = vocabulary["accepted_instruments"].apply(
            lambda x: _review_term(
                ds[var].attrs.get("long_name"),
                x,
                regexp=True,
                search_flag=re.IGNORECASE,
            )
        )
        match_instrument_global = vocabulary["accepted_instruments"].apply(
            lambda x: _review_term(
                f"{ds.attrs.get('instrument_type')} {ds.attrs.get('instrument_model')}".strip(),
                x,
                regexp=True,
                search_flag=re.IGNORECASE,
            )
        )
        return vocabulary.loc[
            match_vocabulary
            & match_code
            & match_units
            & match_scale
            & (match_instrument | match_instrument_global)
        ]

    # Generate Standardized Attributes from vocabulary table
    # # The very first item in the expected columns is the main term to use
    # vocabulary["units"] = vocabulary["accepted_units"].str.split("|").str[0]
    # vocabulary["instrument"] = vocabulary["accepted_instrument"].str.split("|").str[0]

    # Find matching vocabulary
    new_variable_order = []
    for var in ds:
        # Ignore variables with no attributes and flag variables
        if (
            not ds[var].attrs
            or "flag_values" in ds[var].attrs
            or "legacy_gf3_code" not in ds[var].attrs
            or var.startswith(("QCFF", "FFFF"))
        ):
            new_variable_order.append(var)
            continue

        # Retrieve standardize units and scale
        _add_reference_scale()
        matching_terms = _get_matching_vocabularies()

        # If nothing matches, move to the next one
        if matching_terms.empty:
            logger.warning(
                "No matching vocabulary term is available for variable %s: %s and instrument: {'type':%s,'model':%s}",
                ds[var].attrs["legacy_gf3_code"],
                ds[var].attrs,
                ds.attrs.get("instrument_type"),
                ds.attrs.get("instrument_model"),
            )
            new_variable_order.append(var)
            continue

        # Consider only the first organization that has this term
        selected_organization = [
            organization
            for organization in organizations
            if organization in matching_terms["Vocabulary"].tolist()
        ][0]
        matching_terms = matching_terms.query(
            f"Vocabulary == '{selected_organization}'"
        )
        gf3 = GF3Code(ds[var].attrs["legacy_gf3_code"])
        # Generate new variables and update original variable attributes from vocabulary
        for _, row in matching_terms.iterrows():
            # Make a copy of original variable
            if row["variable_name"]:
                # Apply suffix number of original variable
                if gf3 and gf3.code not in ("FLOR"):
                    new_variable = update_variable_index(
                        row["variable_name"], gf3.index
                    )
                else:
                    # If variable already exist within dataset and is gf3.
                    # Increment the trailing number until no similar named variable exist.
                    if row["variable_name"] in ds and gf3:
                        new_variable = None
                        trailing_number = 2
                        while new_variable is None or new_variable in ds:
                            new_variable = update_variable_index(
                                row["variable_name"], trailing_number
                            )
                            trailing_number += 1
                    else:
                        new_variable = row["variable_name"]

                # Generate new variable
                if row["apply_function"]:
                    input_args = []
                    extra_args = re.search(r"lambda (.*):", row["apply_function"])
                    if extra_args:
                        for item in extra_args[1].split(","):
                            if item in var:
                                input_args.append(ds[var])
                            elif item in ds:
                                input_args.append(ds[item])
                            else:
                                input_args.append(item)

                    ds[new_variable] = xr.apply_ufunc(
                        eval(row["apply_function"]), *tuple(input_args), keep_attrs=True
                    )
                    ds.attrs["history"] += history_input(
                        f"Add Parameter: {new_variable} = {row['apply_function']}"
                    )
                else:
                    ds[new_variable] = ds[var].copy()
                    if var != new_variable:
                        ds.attrs["history"] += history_input(
                            f"Add Parameter: {new_variable} = {var}"
                        )

                new_attrs = ds[new_variable].attrs
                new_variable_order.append(new_variable)
            else:
                # Apply vocabulary to original variable
                new_attrs = ds[var].attrs
                new_variable_order.append(var)

            # Retrieve all attributes in vocabulary that have something
            new_attrs.update(row[vocabulary_attribute_list].dropna().to_dict())

            # If original data has units but vocabulary doesn't require one drop the units
            if "units" in new_attrs and row["units"] is None:
                new_attrs.pop("units")

            # Update sdn_parameter_urn and long_name terms available
            if (
                "sdn_parameter_urn" in new_attrs
                and "legacy_gf3_code" in new_attrs
                and gf3.code not in ("FLOR")
            ):
                new_attrs["sdn_parameter_urn"] = update_variable_index(
                    new_attrs["sdn_parameter_urn"], gf3.index
                )
                # Add index to long name if bigger than 1
                if gf3.index > 1:
                    new_attrs["long_name"] += f", {gf3.index}"

    dropped_variables = [var for var in ds if var not in new_variable_order]
    if dropped_variables:
        ds.attrs["history"] += history_input(
            f"Drop Parameters: {','.join(dropped_variables)}"
        )
    return ds[new_variable_order]
