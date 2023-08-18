"""
Fisheries and Ocean Canada - Pacific Region - Institute of Ocean Sciences
"""

import logging

import xarray
from cioos_data_transform.IosObsFile import CurFile, GenFile

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
    "cur",
)


def shell(filename: str) -> xarray.Dataset:
    """Parse DFO-IOS Shell format

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
