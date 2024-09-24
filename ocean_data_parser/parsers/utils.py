import json
import logging
import re
from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
import xarray as xr

from ocean_data_parser import __version__

logger = logging.getLogger(__name__)

time_variables_default_encoding = {
    "units": "seconds since 1970-01-01T00:00:00",
    "dtype": "float64",
}

object_variables_default_encoding = {"dtype": "str"}


def test_attribute_names(dataset):
    """Test if attributes names are valid"""
    attribute_checker = re.compile(r"[a-zA-Z_\$][a-zA-Z0-9_\.\@\$]*")
    invalid_global_attributes = []
    invalid_variable_attributes = []
    for key in dataset.attrs:
        if not attribute_checker.fullmatch(key):
            invalid_global_attributes.append(key)
    for variable in dataset:
        for key in dataset[variable].attrs:
            if not attribute_checker.fullmatch(key):
                invalid_variable_attributes.append(f"{variable} -> {key}")
    error_msg = []
    if invalid_global_attributes:
        error_msg += [
            "Invalid global attributes names: %s",
            ", ".join(invalid_global_attributes),
        ]
    if invalid_variable_attributes:
        error_msg += [
            "Invalid variable attributes names: %s",
            ", ".join(invalid_variable_attributes),
        ]
    if error_msg:
        raise ValueError("\n".join(error_msg))


def rename_variables_to_valid_netcdf(dataset):
    def _transform(variable_name):
        variable_name = re.sub(r"[\(\)\-\s]+", "_", variable_name.strip())
        return re.sub(r"^_|_$", "", variable_name)

    return dataset.rename({key: _transform(key) for key in dataset})


def get_history_handler():
    """Generate a history handler to be use to generate a CF History attribute"""
    nc_logger = StringIO()
    nc_handler = logging.StreamHandler(nc_logger)
    nc_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    nc_handler.setLevel(logging.INFO)
    return nc_logger, nc_handler


def _consider_attribute(value):
    if value is pd.NA or value is None:
        return False
    elif isinstance(value, (dict, tuple, list, np.ndarray)):
        return len(value) > 0
    return (pd.notnull(value) or value in (0, 0.0)) and value != ""


def standardize_attributes(attrs) -> dict:
    """Standardize attributes with the following steps:
        - datetime, timestamps -> ISO format text string
        - dict ->  JSON strings
        - list -> np.array
        - bool -> str True/False
        - Drop empty attributes pd.isna !=0 and ==""

    Args:
        attrs (dict): Attributes dictionary

    Returns:
        dict: Standardized dictionary
    """

    def _encode_attribute(value):
        if isinstance(value, bool):
            return str(value)
        elif isinstance(value, (str, int, float)):
            return value
        elif isinstance(value, dict):
            return json.dumps(value)
        elif isinstance(value, (list, tuple)) and len(value) == 0:
            # ignore empty lists
            return
        elif isinstance(value, (list, tuple)) and all(
            isinstance(item, (int, float)) for item in value
        ):
            return np.array(value)
        elif isinstance(value, (list, tuple)):
            return json.dumps(value)
        elif type(value) in (datetime, pd.Timestamp):
            return value.isoformat().replace("+00:00", "Z")
        elif isinstance(value, np.ndarray):
            return value
        else:
            logger.warning("Unknown attribute type: %s", type(value))
            return value

    return {
        attr: _encode_attribute(value)
        for attr, value in attrs.items()
        if _consider_attribute(value)
    }


def generate_variables_encoding(
    ds: xr.Dataset,
    variables: list = None,
    object_variables_encoding=None,
    time_variables_encoding: dict = None,
    utc: bool = True,
):
    """Generate time variables encoding

    Args:
        ds (xr.Dataset): Dataset
        variables (list, optional): List of time variables to encode.
            Defaults to detect automatically datetime/timestamp variables.
        object_varaibles_encoding = Encoding to apply object variables.
            Defaults to: dtype: str
        time_variables_encoding (dict, optional): Encoding to apply.
            Defaults to:
                + units="seconds since 1970-01-01T00:00:00"
                + dtype="float64"
        utc (bool, optional): Assign UTC timezone and converte
            timezone aware timestamps to UTC. Defaults to True.

    Returns:
        xr..Dataset: Dataset with encoding attribute generated.
    """

    for var in variables or ds.variables:
        ds.encoding[var] = {}
        if "datetime" in ds[var].dtype.name:
            ds[var].encoding.update(
                time_variables_encoding or time_variables_default_encoding
            )
            if "tz" in ds[var].dtype.name or utc:
                ds[var].encoding["units"] += "Z"
            ds[var].attrs.pop("units", None)
        elif isinstance(ds[var].dtype, object) and isinstance(
            ds[var].item(0), pd.Timestamp
        ):
            timezone_aware = bool(ds[var].item(0).tz)
            var_attrs = ds[var].attrs
            if ds[var].size == 1:
                ds[var] = ds[var].item(0).tz_convert(None)
            else:
                ds[var] = (
                    ds[var].dims,
                    pd.to_datetime(ds[var].values, utc=timezone_aware).tz_convert(None),
                )
            ds[var].attrs = var_attrs
            ds[var].encoding.update(
                time_variables_encoding or time_variables_default_encoding
            )
            ds[var].attrs.pop("units", None)
            if timezone_aware:
                ds[var].attrs["timezone"] = "UTC"
                ds[var].encoding["units"] += "Z"

        elif ds[var].dtype.name == "object":
            ds[var].encoding.update(
                object_variables_encoding or object_variables_default_encoding
            )
    return ds


def sort_attributes(attrs: dict, attribute_order: list) -> dict:
    """Sort attributes by given order.

    Args:
        attrs (dict): Attributes dictionary
        attribute_order (list): List order to sort the attributes by.
           Attributes not present within this list will
           be sorted alphabetically after the known attributes.

    Returns:
        dict: Sorted attributes
    """
    attrs_output = {attr: attrs[attr] for attr in attribute_order if attr in attrs}
    unknown_order_attrs = dict(
        sorted([attr for attr in attrs.items() if attr not in attribute_order])
    )
    return {**attrs_output, **unknown_order_attrs}


def standardize_dataset(
    ds: xr.Dataset, time_variables_encoding: dict = None, utc: bool = True
) -> xr.Dataset:
    """Standardize dataset to be easily serializable to netcdf
    and compatible with ERDDAP. Apply the following steps:
        - Generate geospatial attributes
        - Apply standardize_variable_attributes
        - Apply standardize_global_attributes
        - Define time variables encoding
        - Verify attribute names

    Args:
        ds (xr.Dataset): Dataset to standardized
        time_variables_encoding (dict, optional): Time variables encoding.
            Defaults to
                + units="seconds since 1970-01-01T00:00:00"
                + dtype="float64"
        utc (bool, optional): Timestamps are UTC or standardize to UTC. Defaults to True.

    Returns:
        xr.Dataset: Standardized dataset
    """
    # Add version to the dataset global attributes and history
    ds.attrs["ocean_data_parser_version"] = __version__  # Add version to the dataset

    ds.attrs["history"] = ds.attrs.get("history", "")
    ds.attrs["history"] += (
        f"{datetime.utcnow().isoformat()} Generated with ocean_data_parser v{__version__}\n"
    )

    ds = get_spatial_coverage_attributes(ds, utc=utc)
    ds = standardize_variable_attributes(ds)
    ds.attrs = standardize_global_attributes(ds.attrs)
    ds = generate_variables_encoding(
        ds, time_variables_encoding=time_variables_encoding, utc=utc
    )
    test_attribute_names(ds)
    return ds


def standardize_global_attributes(attrs):
    """Standardize global attributes order"""
    attrs = standardize_attributes(attrs)
    return sort_attributes(attrs, global_attributes_order)


def standardize_variable_attributes(ds):
    """
    Method to generate simple generic variable attributes and reorder attributes in a consistent order.
    """
    for var in ds.variables:
        # Generate min/max values attributes
        if (
            ds[var].dtype in [float, int, "float32", "float64", "int64", "int32"]
            and "flag_values" not in ds[var].attrs
            and ds[var].size > 0
        ):
            ds[var].attrs["actual_range"] = np.array(
                np.array((ds[var].min().item(0), ds[var].max().item(0))).astype(
                    ds[var].dtype
                )
            )
        ds[var].attrs = standardize_attributes(ds[var].attrs)
        ds[var].attrs = sort_attributes(ds[var].attrs, variable_attributes_order)
    return ds


def get_spatial_coverage_attributes(
    ds,
    time="time",
    lat="latitude",
    lon="longitude",
    depth="depth",
    utc=False,
):
    """
    This method generates the geospatial and time coverage attributes associated to an xarray dataset.
    """
    # TODO add resolution attributes
    # time
    if time in ds.variables and ds[time].size > 0:
        is_utc = ds[time].attrs.get("timezone") == "UTC" or utc
        ds.attrs.update(
            {
                "time_coverage_start": pd.to_datetime(
                    ds[time].min().item(0), utc=is_utc
                ),
                "time_coverage_end": pd.to_datetime(ds[time].max().item(0), utc=is_utc),
                "time_coverage_duration": pd.to_timedelta(
                    (ds[time].max() - ds[time].min()).values
                ).isoformat(),
            }
        )

    # lat/long
    if (
        lat in ds.variables
        and lon in ds.variables
        and ds[lat].size > 0
        and ds[lon].size > 0
    ):
        ds.attrs.update(
            {
                "geospatial_lat_min": ds[lat].min().item(0),
                "geospatial_lat_max": ds[lat].max().item(0),
                "geospatial_lat_units": ds[lat].attrs.get("units"),
                "geospatial_lon_min": ds[lon].min().item(0),
                "geospatial_lon_max": ds[lon].max().item(0),
                "geospatial_lon_units": ds[lon].attrs.get("units"),
            }
        )

    # depth coverage
    if depth in ds.variables and ds[depth].size > 0:
        ds["depth"].attrs["positive"] = ds["depth"].attrs.get("positive", "down")
        ds.attrs.update(
            {
                "geospatial_vertical_min": ds[depth].min().item(0),
                "geospatial_vertical_max": ds[depth].max().item(0),
                "geospatial_vertical_units": ds[depth].attrs["units"],
                "geospatial_vertical_positive": "down",
            }
        )

    return ds


def convert_datetime_str(time_str: str, **to_datetime_kwargs) -> pd.Timestamp:
    """Parse time stamp string  to a pandas Timestamp"""
    date_format = None
    if time_str is None:
        return pd.NaT

    if re.fullmatch(r"\d\d\d\d-\d\d-\d\d", time_str):
        date_format = "%Y-%m-%d"
    elif re.fullmatch(r"\d\d-\d\d-\d\d\d\d", time_str):
        date_format = "%d-%m-%Y"
    elif re.fullmatch(r"%d-\w\w\w-\d\d\d\d", time_str):
        date_format = "%d-%b-%Y"
    elif re.fullmatch(r"\d\d-\w\w\w-\d\d", time_str):
        date_format = "%d-%b-%y"
    elif re.fullmatch(r"\d+-\w\w\w-\d\d\d\d", time_str):
        date_format = "%d-%b-%Y"

    if date_format:
        time = pd.to_datetime(time_str, format=date_format, **to_datetime_kwargs)
        if not isinstance(time, pd.Timestamp):
            logger.warning("Failed to parse datetime: %s", time_str)
        return time
    logger.warning("Unknown time format: %s", time_str)
    return pd.to_datetime(time_str, **to_datetime_kwargs)


global_attributes_order = [
    "organization",
    "institution",
    "institution_fr",
    "country",
    "ioc_country_code",
    "iso_3166_country_code",
    "ices_edmo_code",
    "sdn_institution_urn",
    "program",
    "project",
    "infoUrl",
    "title",
    "processing_level",
    "summary",
    "comments",
    "platform",
    "sdn_platform_urn",
    "platform_imo_number",
    "platform_vocabulary",
    "cruise_name",
    "cruise_number",
    "cruise_description",
    "chief_scientist",
    "mission_start_date",
    "mission_end_date",
    "platform",
    "platform_name",
    "platform_owner",
    "platform_type",
    "country_of_origin",
    "ices_platform_code",
    "wmo_platform_code",
    "call_sign",
    "id",
    "naming_authority",
    "original_filename",
    "event_number",
    "profile_direction",
    "event_start_time",
    "event_end_time",
    "initial_latitude",
    "initial_longitude",
    "station",
    "instrument",
    "instrument_type",
    "instrument_model",
    "instrument_serial_number",
    "instrument_vocabulary",
    "instrument_description",
    "instrument_manufacturer_header",
    "date_created",
    "creator_name",
    "creator_email",
    "creator_institution",
    "creator_country",
    "creator_sector",
    "creator_url",
    "creator_type",
    "publisher_name",
    "publisher_email",
    "publisher_country",
    "publisher_url",
    "publisher_type",
    "publisher_institution",
    "date_modified",
    "date_issued",
    "history",
    "time_coverage_start",
    "time_coverage_end",
    "time_coverage_duration",
    "time_coverage_resolution",
    "geospatial_lat_min",
    "geospatial_lat_max",
    "geospatial_lat_units",
    "geospatial_lon_min",
    "geospatial_lon_max",
    "geospatial_lon_units",
    "geospatial_vertical_min",
    "geospatial_vertical_max",
    "geospatial_vertical_units",
    "geospatial_vertical_positive",
    "geospatial_vertical_resolution",
    "cdm_data_type",
    "cdm_profile_variables",
    "keywords",
    "acknowledgement",
    "license",
    "keywords_vocabulary",
    "standard_name_vocabulary",
    "Conventions",
    "ocean_data_parser_version",
]

variable_attributes_order = [
    "long_name",
    "units",
    "time_zone",
    "scale",
    "standard_name",
    "sdn_parameter_name",
    "sdn_parameter_urn",
    "sdn_uom_urn",
    "sdn_uom_name",
    "ioos_category",
    "gf3_code",
    "source",
    "reference",
    "comments",
    "definition",
    "ancillary_variables",
    "cell_method",
    "actual_range",
    "valid_range",
    "value_min",
    "value_max",
    "mising_value",
    "_FillValue",
    "fileAccessBaseUrl",
    "_Encoding",
    "grid_mapping",
]
