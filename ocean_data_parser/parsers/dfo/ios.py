"""Fisheries and Ocean Canada - Pacific Region - Institute of Ocean Sciences."""

import logging

import xarray

from ocean_data_parser.parsers.dfo.ios_source.ios_obs_file import IosFile
from ocean_data_parser.parsers.utils import standardize_dataset

logger = logging.getLogger(__name__)
HANDLED_DATA_TYPES = (
    "tob",
    "drf",
    "ane",
    "ubc",
    "loop",
    "ctd",
    "mctd",
    "bot",
    "che",
    "med",
    "cur",
)
TRACJECTORY_DATA_TYPES = ("tob", "drf", "loop")


def shell(fname: str, config: dict = {}) -> xarray.Dataset:
    """Parse DFO-IOS Shell format.

    Args:
        fname (str): file path
        config (dict, optional): Configuration. Defaults to {}.

    Raises:
        RuntimeError: Incompatible file format.

    Returns:
        xarray.Dataset: Parsed xarray dataset
    """
    # read file based on file type
    extension = fname.rsplit(".", 1)[1].lower()
    if extension not in HANDLED_DATA_TYPES:
        raise RuntimeError(f"Package is not compatible yet with {extension} files.")

    # Load file
    ios_file = IosFile(filename=fname)
    ios_file.import_data()

    # Fix some issues associated with some files
    ios_file.fix_variable_names()

    logger.debug("Imported data successfully!")
    if extension not in TRACJECTORY_DATA_TYPES:
        ios_file.assign_geo_code(config.get("geographic_area", {}))

    ios_file.add_ios_vocabulary()
    ds = ios_file.to_xarray()
    ds.attrs.update(config.get("global_attributes", {}))
    return standardize_dataset(ds)
