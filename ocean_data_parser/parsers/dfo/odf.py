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

from ocean_data_parser.parsers.dfo.odf_source.process import parse_odf

logger = logging.getLogger(__name__)


odf_global_attributes = {
    "organization": "Fisheries and Ocean Canada - Pêche et Océan Canada",
    "institution": "DFO BIO",
    "country": "Canada",
    "ioc_country_code": 18,
    "iso_3166_country_code": "CA",
    "platform_vocabulary": "https://vocab.nerc.ac.uk/search_nvs/C17/",
    "instrument_vocabulary": "https://vocab.nerc.ac.uk/search_nvs/L22/",
    "Conventions": "CF-1.6,CF-1.7,CF-1.8,ACDD1.1,ACDD-1.3,IOOS-1.2",
    "standard_name_vocabulary": "CF Standard Name Table v78",
}

mli_global_attributes = {
    "institution": "DFO MLI",
    "ices_edmo_code": 4160,
    "sdn_institution_urn": "SDN:EDMO::4160",
    "infoUrl": "https://www.qc.dfo-mpo.gc.ca/fr/institut-maurice-lamontagne",
    "naming_authority": "ca.gc.mli",
    "creator_name": "Maurice Lamontagne Insitute (MLI)",
    "creator_institution": "Maurice Lamontagne Insitute (MLI)",
    "creator_email": "info@dfo-mpo.gc.ca",
    "creator_country": "Canada",
    "creator_sector": "gov_federal",
    "creator_url": "info@dfo-mpo.gc.ca",
    "creator_type": "institution",
}
bio_global_attributes = {
    "institution": "DFO BIO",
    "ices_edmo_code": 1811,
    "sdn_institution_urn": "SDN:EDMO::1811",
    "infoUrl": "https://www.bio.gc.ca/",
    "naming_authority": "ca.gc.bio",
    "creator_name": "Bedford Institute of Oceanography (BIO)",
    "creator_institution": "Bedford Institute of Oceanography (BIO)",
    "creator_email": "bio.datashop@dfo-mpo.gc.ca",
    "creator_country": "Canada",
    "creator_sector": "gov_federal",
    "creator_url": "https://www.bio.gc.ca/",
    "creator_type": "institution",
}


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


def bio_odf(path: str, global_attributes: dict = None) -> xarray.Dataset:
    """Bedford Institute of Ocean ODF format parser

    Args:
        path (str): Path to the odf file to parse
        global_attributes (dict): file specific global attributes
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return odf(
        path,
        institution="BIO",
        global_attributes={**bio_global_attributes, **(global_attributes or {})},
    )


def mli_odf(path: str, global_attributes: dict = None) -> xarray.Dataset:
    """Maurice Lamontagne Institute ODF format parser

    Args:
        path (str): Path to the odf file to parse
        global_attributes (dict): file specific global attributes
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return odf(
        path,
        institution="MLI",
        global_attributes={**mli_global_attributes, **(global_attributes or {})},
    )


def odf(path: str, institution: str = None, global_attributes: dict = None):
    """ODF format parser

    Args:
        path (str): Path to the odf file to parse
        institution (str): Institution to use for the vocabulary mapping
        global_attributes (dict): file specific global attributes
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return parse_odf(
        path,
        institution=institution,
        global_attributes={**odf_global_attributes, **(global_attributes or {})},
    )
