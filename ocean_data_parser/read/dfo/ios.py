import logging
from cioos_data_transform.IosObsFile import CurFile, GenFile

logger = logging.getLogger(__name__)
HANDLED_DATA_TYPES = ("tob", "drf", "ane", "ubc", "loop", "ctd", "mctd", "bot", "che")


def shell(filename):
    """Read IOS Shell format to xarray"""
    extension = filename.rsplit(".", 1)[1]
    if extension == "cur":
        fdata = CurFile(filename=filename, debug=False)
    elif extension in HANDLED_DATA_TYPES:
        fdata = GenFile(filename=filename, debug=False)
    else:
        logger.error("Filetype not understood!")

    fdata.add_ios_vocabulary()
    return fdata.to_xarray()
