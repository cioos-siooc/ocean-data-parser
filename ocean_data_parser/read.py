"""This module contains all the different tools needed to parse a file."""

import logging
import re
import sys
from importlib import import_module
from pathlib import Path
from typing import Union

import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)


def detect_file_format(file: str, encoding: str = "UTF-8") -> str:
    """Detect corresponding data parser for a given file.

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
    if file.is_dir():
        raise ValueError(f"Directory provided instead of a file: {file}")
    ext = file.suffix[1:]

    with open(file, encoding=encoding, errors="ignore") as file_handle:
        header = ""
        for _ in range(10):
            try:
                header += next(file_handle)
            except StopIteration:
                break

    # Detect the right file format
    if ext == "nc":
        parser = "netcdf"
    elif ext == "btl" and "* Sea-Bird" in header:
        parser = "seabird.btl"
    elif ext == "cnv" and "* Sea-Bird" in header:
        parser = "seabird.cnv"
    elif ext == "csv" and re.search("electricblue", header):
        parser = "electricblue.csv"
    elif ext == "csv" and (
        "Plot Title" in header
        or (re.search(r"Serial Number:\s*\d+\s*", header) and "Host Connect" in header)
        or (re.search(r"\"LGR S\/N:\s*[\d\-]+", header) and "Date Time, GMT" in header)
        or ('#,"Date Time, GMT' in header and "(LGR S/N: " in header)
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
        parser = "star_oddi.dat"
    elif ext == "int" and "% Cruise_Number:" in header:
        parser = "amundsen.int_format"
    elif ext == "csv" and "% Cruise_Number:" in header:
        parser = "amundsen.csv_format"
    elif "*IOS HEADER VERSION" in header:
        parser = "dfo.ios.shell"
    elif ext == "pcnv":
        parser = "dfo.nafc.pcnv"
    elif ext[0] == "p" and "NAFC_Y2K_HEADER" in header:
        parser = "dfo.nafc.pfile"
    elif ext.upper() == "ODF" and re.search(
        r"COUNTRY_INSTITUTE_CODE\s*=\s*1810", header
    ):
        parser = "dfo.odf.bio_odf"
    elif (
        ext.upper() == "ODF"
        and re.search(r"COUNTRY_INSTITUTE_CODE\s*=\s*1830", header)
        or re.search(r"COUNTRY_INSTITUTE_CODE\s*=\s*CaIML", header)
    ):
        parser = "dfo.odf.mli_odf"
    elif ext.upper() == "ODF" and re.search(r"Ismer\/Québec-Océan", header):
        parser = "dfo.odf.as_qo_odf"
    elif ext.upper() == "ODF":
        logger.warning(
            "Unable to detect ODF related institution code (IML=1830/CaIML;BIO=1810) from header: %s",
            header,
        )
        logger.warning("Default to MLI ODF")
        parser = "dfo.odf.odf"
    elif (
        ext.lower() in ("ctd", "bot", "che", "drf", "cur", "loop", "tob")
        and "*IOS HEADER VERSION" in header
    ):
        parser = "dfo.ios.shell"
    elif ext == "MON":
        parser = "van_essen_instruments.mon"
    elif ext == "txt" and re.match(r"\d+\-\d+\s*\nOS REV\:", header):
        parser = "pme.txt"
    elif ext == "txt" and re.match(r"Model\=.*\nFirmware\=.*\nSerial\=.*", header):
        parser = "rbr.rtext"
    elif ext == "txt" and "Front panel parameter change:" in header:
        parser = "sunburst.super_co2_notes"
    elif ext == "txt" and "CO2 surface underway data" in header:
        parser = "sunburst.super_co2"
    elif all(re.search(r"\$.*,.*,", line) for line in header.split("\n") if line):
        parser = "nmea.file"
    elif ext == "xlsx":
        excel_file = pd.ExcelFile(file)
        sheet_names = excel_file.sheet_names
        if (
            all(sheet in sheet_names for sheet in ("Data", "Events", "Details"))
            and "HOBOconnect"
            in excel_file.parse("Details", names=[0, 1, 2, 3])[3].tolist()
        ):
            parser = "onset.xlsx"
    else:
        raise ImportError(f"Unable to match file to a specific data parser: {file}")

    logger.debug("Selected parser: %s", parser)
    return parser


def import_parser(parser: str):
    if parser == "netcdf":
        return xr.open_dataset

    read_module, filetype = parser.rsplit(".", 1)
    imported_modules = sys.modules
    module_full_name = f"ocean_data_parser.parsers.{read_module}"

    if module_full_name not in imported_modules:
        logger.info("Import module: %s", module_full_name)
        mod = import_module(module_full_name)
    else:
        logger.debug("Module already imported: %s", module_full_name)
        mod = imported_modules[module_full_name]
    return getattr(mod, filetype)


def file(
    path: str,
    parser: str = None,
    global_attributes=None,
    **kwargs: Union[str, int, float],
) -> xr.Dataset:
    """Load compatible file format as an xarray dataset.

    ```python
    from ocean_data_parser import read

    ds = read.file('path_to_file.cnv')
    ```

    Args:
        path (str): File path
        parser (str, optional): Parser to use.
                Defaults to auto `detect_file_format` output if None
        global_attributes (dict, optional): Global attributes to add to the dataset.
        **kwargs: Keyword arguments to pass to the parser

    Returns:
        xarray.Dataset: Parsed xarray dataset for provided file
    """
    # Review the file format if no parser is specified
    if parser is None:
        parser = detect_file_format(path)

    # Load the appropriate parser and read the file
    parser_func = import_parser(parser) if isinstance(parser, str) else parser
    ds = parser_func(path, **(kwargs or {}))
    if global_attributes:
        ds.attrs.update(global_attributes)
    return ds
