import logging
import re
from importlib import import_module
from pathlib import Path

from xarray import Dataset

logger = logging.getLogger(__name__)


def detect_file_format(file: str, encoding: str = "UTF-8") -> str:
    """Detect for a given file, which parser should be used to parse it.

    The parser suggestion is based on the file extension and the
    first few lines of the file itself.

    Args:
        file (str): Path to the file
        encoding (str, optional): Encoding use to parse file. Defaults to "UTF-8".

    Returns:
        str: Parser compatible with this file format
    """
    # Retrieve file extension and the first few lines of the file header
    file = Path(file)
    ext = file.suffix[1:]
    with open(file, encoding=encoding, errors="ignore") as file_handle:
        header = "".join((next(file_handle) for _ in range(5)))

    # Detect the right file format
    if ext == "btl" and "* Sea-Bird" in header:
        parser = "seabird.btl"
    elif ext == "cnv" and "* Sea-Bird" in header:
        parser = "seabird.cnv"
    elif ext == "csv" and re.search("electricblue", header):
        parser = "electricblue.csv"
    elif ext == "csv" and (
        "Plot Title" in header
        or (re.search(r"Serial Number:\s*\d+\s*", header) and "Host Connect" in header)
    ):
        parser = "onset.csv"
    elif (
        ext == "csv"
        and "time, action, id, version, name, status, code, sampling interval (s), "
        + "sampling resolution (C), samples, time diff (s), start time, lat, long, accuracy, device"
        in header
    ):
        parser = "electricblue.log_csv"
    elif ext == "DAT" and "Version	SeaStar" in header:
        parser = "star_oddi.DAT"
    elif ext == "geojson":
        parser = "geojson"
    elif ext == "int" and "% Cruise_Number:" in header:
        parser = "amundsen.int_format"
    elif ext[0] == "p" and "NAFC_Y2K_HEADER" in header:
        parser = "dfo.nafc.pfile"
    elif ext == "ODF" and re.search(r"COUNTRY_INSTITUTE_CODE\s*=\s*1810", header):
        parser = "dfo.odf.bio_odf"
    elif (
        ext == "ODF"
        and re.search(r"COUNTRY_INSTITUTE_CODE\s*=\s*1830", header)
        or re.search(r"COUNTRY_INSTITUTE_CODE\s*=\s*CaIML", header)
    ):
        parser = "dfo.odf.mli_odf"
    elif ext == "ODF":
        logger.warning(
            "Unable to detect ODF related institution code (IML=1830/CaIML;BIO=1810) from header: %s",
            header,
        )
        logger.warning("Default to MLI ODF")
        parser = "dfo.odf.mli_odf"
    elif ext == "MON":
        parser = "van_essen_instruments.mon"
    elif ext == "txt" and re.match(r"\d+\-\d+\s*\nOS REV\:", header):
        parser = "pme.minidot_txt"
    elif ext == "txt" and re.match(r"Model\=.*\nFirmware\=.*\nSerial\=.*", header):
        parser = "rbr.rtext"
    elif ext == "txt" and "Front panel parameter change:" in header:
        parser = "sunburst.superCO2_notes"
    elif ext == "txt" and "CO2 surface underway data" in header:
        parser = "sunburst.superCO2"
    elif all(re.search("\$.*,.*,", line) for line in header.split("\n") if line):
        parser = "nmea.file"
    else:
        raise ImportError("Unable to match file to a specific data parser")

    logger.info("Selected parser: %s", parser)
    return parser


def load_parser(parser: str):
    read_module, filetype = parser.rsplit(".", 1)
    logger.info("Import module: ocean_data_parser.parses.%s", read_module)
    mod = import_module(f"ocean_data_parser.parsers.{read_module}")
    return getattr(mod, filetype)


def file(path: str, parser: str = None, **kwargs) -> Dataset:
    """Automatically detect file format and load it as an xarray dataset.

    Args:
        path (str): path to file to parse
        parser (str, optional): Give parser to use to parse the given data.
                Defaults to auto mode which is looking at the extension
                and file header to asses the appropriate parser to use.
        **kwargs: extra keyword arguments to pass to parser.

    Returns:
        xarray.Dataset: Parsed xarray dataset for provided file
    """
    # Review the file format if no parser is specified
    if parser is None:
        parser = detect_file_format(path)

    # Load the appropriate parser and read the file
    parser_func = load_parser(parser) if isinstance(parser, str) else parser
    return parser_func(path, **kwargs)