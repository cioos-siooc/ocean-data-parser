"""odf_parser is a module that regroup a different set of tools used to
parse the ODF format which is use, maintain and developped
by the DFO offices BIO and MLI.
"""

import logging
import re
from datetime import datetime, timezone

import gsw_xarray as gsw
import pandas as pd

from ocean_data_parser.vocabularies.load import dfo_odf_vocabulary

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

odf_vocabulary = dfo_odf_vocabulary()
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
ORIGINAL_PREFIX_VAR_ATTRIBUTE = "original_"


class GF3Code:
    """ODF GF3 Class split terms in their different components and
    standardize the convention (CODE_XX).
    """

    def __init__(self, code):
        self.code = re.search(r"^[^_]*", code)[0]
        index = re.search(r"\d+$", code)
        self.index = int(index[0]) if index else 1
        self.name = f"{self.code}_{self.index:02}" if index else self.code


def _convert_odf_time(time_string: str) -> pd.Timestamp:
    """Convert ODF timestamps to a datetime object."""
    if time_string == "17-NOV-1858 00:00:00.00" or time_string is None:
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
    """Genereate a CF standard history line: Timstamp comment."""
    return f"{date.strftime('%Y-%m-%dT%H:%M:%SZ')} {comment}\n"


def read(filename, encoding_format="Windows-1252"):
    """Read_odf
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
        - else string.
        """
        # Drop quotes and comma
        value = re.sub(r"^'|,$|',$|'$", "", value)

        # Convert numerical values to float and integers
        if re.match(r"[-+]{0,1}\d+\.\d+$", value):
            return None if value.startswith(("-99.9", "-999.9")) else float(value)
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
                key, value = (item.strip() for item in line.split("=", 1))
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
        variable_attributes = {}
        time_columns = []
        # Variable names and related attributes
        for att in metadata["PARAMETER_HEADER"]:
            # Generate variable name
            var_name = GF3Code(att.get("CODE") or att.get("WMO_CODE")).name
            if var_name is None:
                raise RuntimeError("Unrecognizable ODF variable attributes")

            # Generate variable attributes
            variable_attributes[var_name] = {
                "long_name": att.get("NAME"),
                "units": (att.get("UNITS") or "").replace("**", "^"),
                "legacy_gf3_code": var_name,
                "null_value": att["NULL_VALUE"],
                "resolution": 10 ** -att["PRINT_DECIMAL_PLACES"]
                if att["PRINT_DECIMAL_PLACES"] != -99
                else None,
            }
            # Time type column add to time variables to parse by pd.read_csv()
            if var_name.startswith("SYTM") or att["TYPE"] == "SYTM":
                time_columns += [var_name]

        # Read Data with Pandas
        df = pd.read_csv(
            f,
            delimiter=r"\s+",
            quotechar="'",
            header=None,
            names=variable_attributes.keys(),
            na_values={
                key: att.pop("null_value") for key, att in variable_attributes.items()
            },
            converters={var: _convert_odf_time for var in time_columns},
            encoding=encoding_format,
        )

    # Review N variables
    if len(df.columns) != len(metadata["PARAMETER_HEADER"]):
        raise RuntimeError(f"{df.columns} doesn't match ODF parameters")

    # Convert to an xarray
    dataset = df.to_xarray()
    for variable, attrs in variable_attributes.items():
        dataset[variable].attrs = attrs

    return metadata, dataset


def add_vocabulary_attributes(
    ds,
    vocabularies=None,
    add_attributes_existing_variables=True,
    generate_new_vocabulary_variables=True,
):
    """This method is use to retrieve from an ODF variable code, units and units,
    matching vocabulary terms available.
    """

    def _add_scale_attribute():
        """Retrieve scale information from  either units or long_name."""
        scales = {
            "IPTS-48": r"IPTS\-48",
            "IPTS-68": r"IPTS\-68|ITS\-68",
            "ITS-90": r"ITS\-90|TE90",
            "PSS-78": r"PSS\-78|practical.*salinity|psal",
        }
        if var.startswith("TE90"):
            ds[var].attrs["scale"] = "ITS-90"
            return
        for scale, scale_search in scales.items():
            if re.search(
                scale_search, ds[var].attrs.get("units"), re.IGNORECASE
            ) or re.search(scale_search, ds[var].attrs.get("long_name"), re.IGNORECASE):
                ds[var].attrs["scale"] = scale

    def _review_term(term, accepted_terms, regexp=False, search_flag=None):
        """Simple tool to compare "|" separated units in the Vocabulary expected unit list.
        - First unit if any is matching.
        - True if empty or expected to be empty
        - unknown if unit exists but the "accepted_units" input is empty.
        - False if not matching units.
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
        - global instrument_type instrument_model.
        """
        # Among these matching terms find matching ones
        match_vocabulary = odf_vocabulary["Vocabulary"].isin(vocabularies)
        match_code = (
            odf_vocabulary["name"] == ds[var].attrs["legacy_gf3_code"].split("_")[0]
        )
        match_units = odf_vocabulary["accepted_units"].apply(
            lambda x: _review_term(ds[var].attrs.get("units"), x)
        )
        match_scale = odf_vocabulary["accepted_scale"].apply(
            lambda x: _review_term(ds[var].attrs.get("scale"), x)
        )
        match_instrument = odf_vocabulary["accepted_instruments"].apply(
            lambda x: _review_term(
                ds[var].attrs.get("long_name"),
                x,
                regexp=True,
                search_flag=re.IGNORECASE,
            )
        )
        match_instrument_global = odf_vocabulary["accepted_instruments"].apply(
            lambda x: _review_term(
                f"{ds.attrs.get('instrument_type')} {ds.attrs.get('instrument_model')}".strip(),
                x,
                regexp=True,
                search_flag=re.IGNORECASE,
            )
        )
        return odf_vocabulary.loc[
            match_vocabulary
            & match_code
            & match_units
            & match_scale
            & (match_instrument | match_instrument_global)
        ]

    def _update_variable_index(varname, index):
        """Standardize variables trailing number to two digits."""
        if not varname:
            return varname
        elif varname.endswith(("XX", "01")):
            return f"{varname[:-2]}{index:02}"
        elif varname.endswith(("X", "1")):
            return f"{varname[:-1]}{index:01}"
        return varname

    # Generate Standardized Attributes from vocabulary table
    # # The very first item in the expected columns is the main term to use
    # vocabulary["units"] = vocabulary["accepted_units"].str.split("|").str[0]
    # vocabulary["instrument"] = vocabulary["accepted_instrument"].str.split("|").str[0]

    # Find matching vocabulary
    odf_vocabulary["apply_function"] = odf_vocabulary["apply_function"].fillna("x")
    new_variables_mapping = {}
    new_variables = {}
    new_variables_attributes = {}
    variable_order = []
    for var in ds:
        # Ignore variables with no attributes and flag variables
        if (
            not ds[var].attrs
            or "flag_values" in ds[var].attrs
            or "legacy_gf3_code" not in ds[var].attrs
            or var.startswith(("QCFF", "FFFF"))
        ):
            variable_order.append(var)
            continue

        # Retrieve standardize units and scale
        _add_scale_attribute()
        matching_terms = _get_matching_vocabularies()

        # If nothing matches, move to the next one
        if matching_terms.empty:
            logger.warning(
                (
                    "No matching vocabulary available for gf3=%s: %s"
                    " and instrument: {'type':%s,'model':%s}"
                ),
                ds[var].attrs["legacy_gf3_code"],
                ds[var].attrs,
                ds.attrs.get("instrument_type"),
                ds.attrs.get("instrument_model"),
            )
            variable_order.append(var)
            continue

        # Consider only the first organization that has this term
        selected_organization = [
            organization
            for organization in vocabularies
            if organization in matching_terms["Vocabulary"].tolist()
        ][0]
        matching_terms = matching_terms.query(
            f"Vocabulary == '{selected_organization}'"
        )
        gf3 = GF3Code(var)
        if gf3.code == "FLOR":
            for id, new_var in matching_terms.iterrows():
                variable_name = new_var["variable_name"]
                index = 1
                while variable_name in new_variables:
                    index += 1
                    variable_name = _update_variable_index(variable_name, index)
                matching_terms.loc[id, "variable_name"] = variable_name
                matching_terms.loc[id, "sdn_parameter_urn"] = _update_variable_index(
                    matching_terms.loc[id, "sdn_parameter_urn"], index
                )
                if index > 1:
                    matching_terms.loc[id, "long_name"] += f", {index}"

        else:
            matching_terms.loc[:, "variable_name"] = (
                matching_terms["variable_name"]
                .fillna(var)
                .apply(lambda x: _update_variable_index(x, gf3.index))
            )
            matching_terms.loc[:, "sdn_parameter_urn"] = matching_terms[
                "sdn_parameter_urn"
            ].apply(lambda x: _update_variable_index(x, gf3.index))
            if gf3.index > 1:
                matching_terms.loc[:, "long_name"] += f", {gf3.index}"

        # Add attributes to original variable
        if add_attributes_existing_variables:
            ds[var].attrs.update(
                matching_terms.iloc[-1][vocabulary_attribute_list].dropna().to_dict()
            )

        variable_order += [var]
        if not generate_new_vocabulary_variables:
            continue

        # Generate vocabulary variables
        comment = ""
        variable_order += matching_terms["variable_name"].tolist()
        locals_variables = {
            "gsw": gsw,
            "x": ds[var],
            **{item: ds[item] for item in ds.variables},
        }
        if (
            "latitude" not in ds
            and "longitude" not in ds
            and "LATD_01" in ds
            and "LOND_01" in ds
        ):
            locals_variables["latitude"] = ds["LATD_01"]
            locals_variables["longitude"] = ds["LOND_01"]
            comment += " LATD_01 is used as latitude, LOND_01 is used as longitude"
        new_variables_mapping.update(
            {
                var: f"with x={var} -> {item['apply_function']} = {item['variable_name']}"
                if item["apply_function"] != "x"
                else f"{var} = {item['variable_name']}"
                for _, item in matching_terms.iterrows()
            }
        )
        new_variables.update(
            {
                item["variable_name"]: eval(
                    item["apply_function"], {}, locals_variables
                )
                for _, item in matching_terms.iterrows()
            }
        )
        new_variables_attributes.update(
            {
                item["variable_name"]: {
                    **ds[var].attrs,
                    **item[vocabulary_attribute_list].to_dict(),
                }
                for _, item in matching_terms.iterrows()
            }
        )
    # Include the new variables and their variable attributes
    if generate_new_vocabulary_variables:
        ds = ds.assign(new_variables)
        for variable, attrs in new_variables_attributes.items():
            ds[variable].attrs = attrs

        ds.attrs["history"] += history_input(
            "Generate new vocabulary variables: "
            f"{'; '.join(new_variables_mapping.values())}; {comment}"
        )
    return ds[variable_order]
