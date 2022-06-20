import logging
import xarray as xr
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
