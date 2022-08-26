import json
import logging
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def test_parsed_dataset(ds):
    # instrument_sn
    if "instrument_sn" not in ds and ds.attrs.get("instrument_sn") is None:
        logger.warning("Failed to retrieve instrument serial number")
    elif "instrument_sn" in ds and ds["instrument_sn"].isna().any():
        logger.warning("Some records aren't associated with instrument serial number")

    # time
    if "time" not in ds:
        logger.warning("Missing time variable")


def standardize_dataset(ds):
    """Standardize dataset to be easily serializable to netcdf and compatible with ERDDAP"""

    # TODO Specify encoding for some variables (ex time variables)
    for var in ds:
        ds.encoding[var] = {}
        if "datetime" in ds[var].dtype.name:
            ds[var].encoding.update({"units": "seconds since 1970-01-01 00:00:00"})
            if "tz" in ds[var].dtype.name:
                ds[var].encoding["units"] += "Z"
        elif isinstance(
            ds[var].values[0], pd.Timestamp
        ):  # Timestamp variables or timezone aware
            logger.info("Convert Timestamp variable %s to datetime object", var)
            time_var = pd.to_datetime(ds[var])
            encoding = ds[var].encoding
            encoding.update({"units": "seconds since 1970-01-01 00:00:00"})
            if time_var.tz:
                logger.info("Convert %s to UTC timezone", var)
                time_var = time_var.tz_convert("UTC").tz_convert(None)
                encoding["units"] += "Z"
                ds[var].attrs["timezone"] = "UTC"
            attrs = ds[var].attrs
            ds[var] = (ds[var].dims, time_var)
            ds[var].attrs = attrs
            ds[var].encoding = encoding
        elif ds[var].dtype.name == "object":
            ds[var] = ds[var].astype(str).str.replace("^None$", "")
            ds[var].encoding["dtype"] = "str"

    # Generate geospatial attributes
    # time
    standard_names = {
        ds[var].attrs.get("standard_name"): var
        for var in ds
        if "standard_name" in ds[var].attrs
    }
    time = standard_names.get("time")
    if time:
        t_min = pd.to_datetime(ds[time].min().values)
        t_max = pd.to_datetime(ds[time].max().values)
        dt = t_max-t_min
        ds.attrs.update(
            {
                "time_coverage_start": t_min.isoformat(),
                "time_coverage_end": t_max.isoformat(),
                "time_coverage_duration": dt.isoformat(),
            }
        )

    # lat/long
    lat = standard_names.get("latitude")
    lon = standard_names.get("longitude")
    if lat and lon:
        lat = standard_names["latitude"]
        ds.attrs.update(
            {
                "geospatial_lat_min": ds[lat].min().values,
                "geospatial_lat_max": ds[lat].max().values,
                "geospatial_lat_units": ds[lat].attrs.get("units"),
                "geospatial_lon_min": ds[lon].min().values,
                "geospatial_lon_max": ds[lon].max().values,
                "geospatial_lon_units": ds[lon].attrs.get("units"),
            }
        )

    # depth coverage
    depth = standard_names.get("depth")
    if depth:
        ds.attrs.update(
            {
                "geospatial_vertical_min": ds[depth].min().values,
                "geospatial_vertical_max": ds[depth].max().values,
                "geospatial_vertical_units": ds[depth].attrs.get("units"),
            }
        )

    # Globals
    for att in ds.attrs.keys():
        # Convert dictionaries attributes to json strings
        if isinstance(ds.attrs[att], dict):
            ds.attrs[att] = json.dumps(ds.attrs[att])
        elif type(ds.attrs[att]) in (datetime, pd.Timestamp):
            ds.attrs[att] = ds.attrs[att].isoformat()
        elif type(ds.attrs[att]) in (bool,):
            ds.attrs[att] = str(ds.attrs[att])

    # Drop empty attributes
    ds.attrs = {
        attr: value
        for attr, value in ds.attrs.items()
        if type(value) in (dict, list) or (value and pd.notnull(value))
    }

    # Drop empty variable attributes
    for var in ds:
        ds[var].attrs = {
            attr: value
            for attr, value in ds[var].attrs.items()
            if type(value) in (dict, list)
            or value
            and pd.notnull(value)
            and not isinstance(value, list)
        }
    return ds
