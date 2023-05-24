"""
DFO Module
This module regroups all the different parsers associated with 
the different data formats developped by the different Canadian DFO offices.
"""
from typing import Union
import logging
from ocean_data_parser.read.dfo.odf_source.process import (
    parse_odf,
    read_config,
    save_parsed_odf_to_netcdf,
)

from odf_transform.process import odf_to_xarray, read_config as cioos_odf_config

logger = logging.getLogger(__name__)


def bio_odf_cioos(path: str, config):
    """Read ODF with the CIOOS Data Transform package"""

    config = cioos_odf_config(config)
    config["organisationVocabulary"] = ["BIO", "GF3"]
    return odf_to_xarray(path, config)


def mli_odf_cioos(path: str, config):
    """Read ODF with the CIOOS Data Transform package"""

    config = cioos_odf_config(config)
    config["organisationVocabulary"] = ["MLI", "GF3"]
    return odf_to_xarray(path, config)


def bio_odf(path: str, config: Union[str, dict] = None, output=None):
    """Bedford Institute of Ocean ODF format parser
    Args:
        path (str): Path to the odf file to parse
        config (dict): Configuration parameters used to parse the odf file.
        output (None|netcdf): output to netcdf or output xarray from function
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    if config is None:
        config = read_config(institute="bio")

    config["organisationVocabulary"] = ["BIO", "GF3"]
    ds = parse_odf(path, config=config)
    if output == "netcdf":
        save_parsed_odf_to_netcdf(ds, path, config)
    return ds


def mli_odf(path: str, config: Union[str, dict] = None, output=None):
    """Maurice Lamontagne Institute ODF format parser
    Args:
        path (str): Path to the odf file to parse
        config (dict): Configuration parameters used to parse the odf file.
        output (None|netcdf): output to netcdf or output xarray from function
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    if config is None:
        config = read_config(institute="mli")

    config["organisationVocabulary"] = ["MLI", "GF3"]
    ds = parse_odf(path, config=config)
    if output == "netcdf":
        save_parsed_odf_to_netcdf(ds, path, config)
    return ds
