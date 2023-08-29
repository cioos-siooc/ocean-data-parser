"""
Fisheries and Ocean Canada - Pacific Region - Institute of Ocean Sciences
"""

import logging

import xarray
from cioos_data_transform.IosObsFile import CurFile, GenFile

from ocean_data_parser.parsers.dfo.ios_source.IosObsFile import IosFile

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


def shell_cioos(filename: str) -> xarray.Dataset:
    """Parse DFO-IOS Shell format with the cioos-siooc_data_transform package

    Args:
        filename (str): path to file

    Raises:
        RuntimeError: Failed to read file

    Returns:
        xarray.Dataset
    """
    extension = filename.rsplit(".", 1)[1]
    if extension == "cur":
        fdata = CurFile(filename=filename, debug=False)
    elif extension.lower() in HANDLED_DATA_TYPES:
        fdata = GenFile(filename=filename, debug=False)
    else:
        raise RuntimeError("File type not compatible")

    fdata.import_data()
    fdata.add_ios_vocabulary()
    return fdata.to_xarray()


def shell(fname: str, config: dict = {}) -> xarray.Dataset:
    """Parse DFO-IOS Shell format

    Args:
        fname (str): file path
        config (dict, optional): Configuration. Defaults to {}.

    Raises:
        RuntimeError: _description_
        RuntimeError: _description_

    Returns:
        xarray.Dataset: Parsed xarray dataset
    """
    # read file based on file type
    extension = fname.rsplit(".", 1)[1].lower()
    if extension not in HANDLED_DATA_TYPES:
        raise RuntimeError(f"Package is not compatible yet with {extension} files.")

    # Load file
    fdata = IosFile(filename=fname, debug=False)
    imported = fdata.import_data()
    if not imported:
        raise RuntimeError("Failed to import data")

    logger.debug("Imported data successfully!")
    if extension not in TRACJECTORY_DATA_TYPES:
        fdata.assign_geo_code(config.get("geographic_area", {}))

    fdata.add_ios_vocabulary()
    ds = fdata.to_xarray()
    ds.attrs.update(config.get("global_attributes", {}))
    ds.attrs["source"] = fname
    return ds
