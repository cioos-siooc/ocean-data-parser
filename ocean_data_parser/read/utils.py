import json
import logging
from datetime import datetime
from io import StringIO

import pandas as pd

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


def standardize_dateset(ds):
    """Standardize dataset to be easily serializable to netcdf and compatible with ERDDAP"""
    # Globals
    for att in ds.attrs.keys():
        # Convert dictionaries attributes to json strings
        if isinstance(ds.attrs[att], dict):
            ds.attrs[att] = json.dumps(ds.attrs[att])
        elif type(ds.attrs[att]) in (datetime, pd.Timestamp):
            ds.attrs[att] = ds.attrs[att].isoformat()
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

    # Specify encoding for some variables (ex time variables)
    for var in ds:
        ds.encoding[var] = {}
        if "datetime" in ds[var].dtype.name:
            ds[var].encoding.update({"units": "seconds since 1970-01-01  00:00:00"})
            if "tz" in ds[var].dtype.name:
                ds[var].encoding["units"] += "Z"
    return ds
