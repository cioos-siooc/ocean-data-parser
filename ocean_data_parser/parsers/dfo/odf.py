"""
# Fisheries and Ocean Canada - ODF Format
This module regroups all the tools related to the ODF format actively used by the Fisheries and Ocean Canada offices:

- [Institute Maurice Lamontagne (MLI)](https://www.qc.dfo-mpo.gc.ca/institut-maurice-lamontagne)
- [Bedford Institude of Ocean Sciences (BIO)](https://www.bio.gc.ca/)

"""

import logging
from typing import Union

import xarray
from odf_transform.process import odf_to_xarray
from odf_transform.process import read_config as cioos_odf_config

from ocean_data_parser.parsers.dfo.odf_source.process import (
    parse_odf,
    read_config,
    to_netcdf,
)

logger = logging.getLogger(__name__)


def bio_odf_cioos(path: str, config: Union[dict, str]) -> xarray.Dataset:
    """Read BIO ODF with the CIOOS Data Transform package

    Args:
        path (str): file path to read.
        config (Union[dict, str]): cioos-ioos-data-transform configurations

    Returns:
        xarray.Dataset: CIOOS Compliant Xarray object
    """

    config = cioos_odf_config(config)
    config["organisationVocabulary"] = ["BIO", "GF3"]
    return odf_to_xarray(path, config)


def mli_odf_cioos(path: str, config: Union[dict, str]) -> xarray.Dataset:
    """Read MLI ODF with the CIOOS Data Transform package

    Args:
        path (str): file path to read.
        config (Union[dict, str]): cioos-ioos-data-transform configurations

    Returns:
        xarray.Dataset: CIOOS Compliant Xarray object
    """
    config = cioos_odf_config(config)
    config["organisationVocabulary"] = ["MLI", "GF3"]
    return odf_to_xarray(path, config)


def bio_odf(
    path: str, config: Union[str, dict] = None, output: str = None
) -> xarray.Dataset:
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
        to_netcdf(ds, path, config)
    return ds


def mli_odf(path: str, config: Union[str, dict] = None, output=None) -> xarray.Dataset:
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
        to_netcdf(ds, path, config)
    return ds
