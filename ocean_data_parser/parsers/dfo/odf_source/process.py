"""General module use to convert ODF files into a NetCDF CF, ACDD compliant format."""

import logging
import re
from pathlib import Path

import xarray as xr

import ocean_data_parser.parsers.seabird as seabird
from ocean_data_parser import __version__
from ocean_data_parser.parsers.dfo.odf_source import attributes, flags
from ocean_data_parser.parsers.dfo.odf_source import parser as odf_parser
from ocean_data_parser.parsers.utils import standardize_dataset

no_file_logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(no_file_logger, {"file": None})


FILE_NAME_CONVENTIONS = (
    r"(?P<file_type>[A-Z]+)_(?P<cruise_number>[A-Z0-9]+)_"
    r"(?P<event_number>[^_]+)_(?P<event_identifier1>[^_]+)_"
    r"(?P<event_identifier2>[^_]+).ODF"
)

ODF_COMPATIBLE_DATA_TYPES = [
    "CTD",
    "BT",
    "BOTL",
    "MCTD",
    "XBT",
    "MCM",
    "MADCP",
    "MMOB",
    "MTC",
    "MTG",
    "TCTD",
    "MTR",
    "TSG",
    "PLNKG",
]


def parse_odf(
    odf_path: str,
    global_attributes: dict = None,
    vocabularies: list = None,
    add_attributes_existing_variables: bool = True,
    generate_new_vocabulary_variables: bool = True,
) -> xr.Dataset:
    """Convert an ODF file to an xarray object.

    Args:
        odf_path (str): ODF file path
        global_attributes (dict, optional): Global attribtes to append to dataaset.
            Defaults to None.
        vocabularies (list, optional): Vocabularies to use ['GF3','BIO','IML'].
            Defaults to None.
        add_attributes_existing_variables (bool, optional): Append vocabulary attributes.
            Defaults to True.
        generate_new_vocabulary_variables (bool, optional): Generate vocabulary variables.
            Defaults to True.

    Returns:
        xr.Dataset: Parsed dataset
    """
    # Parse the ODF file with the CIOOS python parsing tool
    metadata, dataset = odf_parser.read(odf_path)

    # Review ODF data type compatible with ODF parser
    if metadata["EVENT_HEADER"]["DATA_TYPE"] not in ODF_COMPATIBLE_DATA_TYPES:
        logger.warning(
            "ODF parser is not yet fully compatible with the ODF Data Type: %s",
            metadata["EVENT_HEADER"]["DATA_TYPE"],
        )

    # Write global and variable attributes
    file_name_attributes = re.search(FILE_NAME_CONVENTIONS, Path(odf_path).name)
    if not file_name_attributes:
        logger.warning(
            "The file name doesn't match an expected naming convention: %s",
            FILE_NAME_CONVENTIONS,
        )
    dataset.attrs = {
        **(file_name_attributes.groupdict() if file_name_attributes else {}),
        **(global_attributes or {}),
        "source": odf_path,
    }
    dataset = attributes.global_attributes_from_header(dataset, metadata)
    dataset.attrs[
        "history"
    ] += f"# Convert ODF to NetCDF with ocean_data_parser V {__version__}\n"

    # Handle ODF flag variables
    dataset = flags.rename_qqqq_flags(dataset)
    dataset = flags.add_flag_attributes(dataset)

    # Generate geographical attributes
    dataset = attributes.generate_coordinates_variables(dataset)

    # Add Vocabulary attributes
    dataset = odf_parser.add_vocabulary_attributes(
        dataset,
        vocabularies=vocabularies,
        add_attributes_existing_variables=add_attributes_existing_variables,
        generate_new_vocabulary_variables=generate_new_vocabulary_variables,
    )

    # Fix flag variables with some issues to map
    dataset = flags.fix_flag_variables(dataset)

    # Instrument specific variables and attributes
    if dataset.attrs["instrument_manufacturer_header"].startswith("* Sea-Bird"):
        dataset = seabird._add_seabird_instruments(
            dataset,
            dataset.attrs["instrument_manufacturer_header"],
            match_by="sdn_parameter_urn",
        )
        dataset = seabird._update_attributes_from_seabird_header(
            dataset, dataset.attrs["instrument_manufacturer_header"]
        )
    # Standardize
    dataset = standardize_dataset(dataset, utc=True)

    # Handle coordinates and dimensions
    coordinates = ["measurement_time", "latitude", "longitude", "depth"]
    dataset = dataset.set_coords([var for var in coordinates if var in dataset])
    dimensions = [
        {"cdm_data_type": "Profile", "variable": "depth"},
        {"cdm_data_type": "Timeseries", "variable": "measurement_time"},
    ]
    for dimension in dimensions:
        if (
            dataset.attrs["cdm_data_type"] == dimension["cdm_data_type"]
            and "index" in dataset
            and dimension["variable"] in dataset
        ):
            dataset = dataset.swap_dims({"index": dimension["variable"]}).drop_vars(
                "index"
            )

    # Log variables available per file
    logger.debug(f"Variable List: {list(dataset)}")

    return dataset
