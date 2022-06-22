import json
import logging
from datetime import datetime

from pandas import Timestamp

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


def standardize_dateset(ds):
    # Globals
    for att in ds.attrs.keys():
        # Convert dictionaries attributes to json strings
        if type(ds.attrs[att]) is dict:
            ds.attrs[att] = json.dumps(ds.attrs[att])
        elif type(ds.attrs[att]) in (datetime, Timestamp):
            ds.attrs[att] = ds.attrs[att].isoformat()

    # TODO Specify encoding for some variables (ex time variables)
    for var in ds:
        ds.encoding[var] = {}
        if "datetime" in ds[var].dtype.name:
            ds.encoding[var].update({"units": "seconds since 1970-01-01T00:00:00"})
            if "tz" in ds[var].dtype.name:
                ds.encoding[var]["units"] += "Z"
    return ds
