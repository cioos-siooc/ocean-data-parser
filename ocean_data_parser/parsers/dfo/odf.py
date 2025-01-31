"""DFO ODF format is a common standard format used by different govermental and academic organisation.

- [Bedford Institute of Ocean Sciences (BIO)](https://www.bio.gc.ca/)
- [Institute Maurice Lamontagne (MLI)](https://www.qc.dfo-mpo.gc.ca/institut-maurice-lamontagne)
- [Marine Institute of science of Rimouski (ISMER)](https://www.ismer.ca/)
"""

import logging

import xarray

from ocean_data_parser.parsers.dfo.odf_source.process import parse_odf

logger = logging.getLogger(__name__)


odf_global_attributes = {
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


def bio_odf(path: str, global_attributes: dict = None) -> xarray.Dataset:
    """Bedford Institute of Ocean ODF format parser.

    Args:
        path (str): Path to the odf file to parse
        global_attributes (dict): file specific global attributes
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return odf(
        path,
        vocabularies=["BIO", "GF3"],
        global_attributes={**bio_global_attributes, **(global_attributes or {})},
    )


def mli_odf(path: str, global_attributes: dict = None) -> xarray.Dataset:
    """Maurice Lamontagne Institute ODF format parser.

    Args:
        path (str): Path to the odf file to parse
        global_attributes (dict): file specific global attributes
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return odf(
        path,
        vocabularies=["MLI", "GF3"],
        global_attributes={**mli_global_attributes, **(global_attributes or {})},
    )


def as_qo_odf(path: str, global_attributes: dict = None) -> xarray.Dataset:
    """AS QO ODF format parser.

    Args:
        path (str): Path to the odf file to parse
        global_attributes (dict): file specific global attributes
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return odf(
        path,
        vocabularies=["AS_QO"],
        global_attributes={**odf_global_attributes, **(global_attributes or {})},
    )


def odf(path: str, vocabularies: list = None, global_attributes: dict = None):
    """ODF format parser.

    Args:
        path (str): Path to the odf file to parse
        vocabularies (str): Vocabulary list to use for the vocabulary mapping
        global_attributes (dict): file specific global attributes
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return parse_odf(
        path,
        vocabularies=vocabularies,
        global_attributes={**odf_global_attributes, **(global_attributes or {})},
    )
