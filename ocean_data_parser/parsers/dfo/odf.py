"""DFO ODF format is a common standard format used by different govermental and academic organisation.

- [Bedford Institute of Ocean Sciences (BIO)](https://www.bio.gc.ca/)
- [Institute Maurice Lamontagne (MLI)](https://www.qc.dfo-mpo.gc.ca/institut-maurice-lamontagne)
- [Marine Institute of science of Rimouski (ISMER)](https://www.ismer.ca/)
"""

import logging

import xarray

from ocean_data_parser.parsers.dfo.odf_source.process import (
    FILE_NAME_CONVENTIONS,
    parse_odf,
)

logger = logging.getLogger(__name__)


odf_global_attributes = {
    "Conventions": "CF-1.6,CF-1.7,CF-1.8,ACDD1.1,ACDD-1.3,IOOS-1.2",
    "standard_name_vocabulary": "CF Standard Name Table v78",
}

mli_global_attributes = {
    "organization": "Fisheries and Ocean Canada - Pêche et Océan Canada",
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
    "country": "Canada",
    "ioc_country_code": 18,
    "iso_3166_country_code": "CA",
    "platform_vocabulary": "https://vocab.nerc.ac.uk/search_nvs/C17/",
    "instrument_vocabulary": "https://vocab.nerc.ac.uk/search_nvs/L22/",
}
bio_global_attributes = {
    "organization": "Fisheries and Ocean Canada - Pêche et Océan Canada",
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
    "country": "Canada",
    "ioc_country_code": 18,
    "iso_3166_country_code": "CA",
    "platform_vocabulary": "https://vocab.nerc.ac.uk/search_nvs/C17/",
    "instrument_vocabulary": "https://vocab.nerc.ac.uk/search_nvs/L22/",
}


as_dfo_global_attributes = {
    "organization": "Quebec Ocean - Laval University - Amundsen Science",
    "Conventions": "CF-1.6,CF-1.7,CF-1.8,ACDD1.1,ACDD-1.3,IOOS-1.2",
    "standard_name_vocabulary": "CF Standard Name Table v78",
}


def bio_odf(
    path: str, global_attributes: dict = None, encoding="Windows-1252"
) -> xarray.Dataset:
    """Bedford Institute of Ocean ODF format parser.

    Args:
        path (str): Path to the odf file to parse
        global_attributes (dict): file specific global attributes
        encoding (str): Encoding format of the file (default: Windows-1252)

    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return odf(
        path,
        vocabularies=["BIO", "GF3"],
        global_attributes={**bio_global_attributes, **(global_attributes or {})},
        encoding=encoding,
    )


def mli_odf(
    path: str, global_attributes: dict = None, encoding="Windows-1252"
) -> xarray.Dataset:
    """Maurice Lamontagne Institute ODF format parser.

    Args:
        path (str): Path to the odf file to parse
        global_attributes (dict): file specific global attributes
        encoding (str): Encoding format of the file (default: Windows-1252)

    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return odf(
        path,
        vocabularies=["MLI", "GF3"],
        global_attributes={**mli_global_attributes, **(global_attributes or {})},
        encoding=encoding,
    )


def as_qo_odf(
    path: str, global_attributes: dict = None, encoding="UTF-8"
) -> xarray.Dataset:
    """AS QO ODF format parser.

    Args:
        path (str): Path to the odf file to parse
        global_attributes (dict): file specific global attributes
        encoding (str): Encoding format of the file (default: UTF-8)

    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return odf(
        path,
        vocabularies=["AS_QO"],
        global_attributes={**as_dfo_global_attributes, **(global_attributes or {})},
        encoding=encoding,
        filename_convention=None,  # TODO there was maybe a convention for AS QO
    )


def odf(
    path: str,
    vocabularies: list = None,
    global_attributes: dict = None,
    encoding: str = "Windows-1252",
    filename_convention=FILE_NAME_CONVENTIONS,
) -> xarray.Dataset:
    """ODF format parser.

    Args:
        path (str): Path to the odf file to parse
        vocabularies (str): Vocabulary list to use for the vocabulary mapping
        global_attributes (dict): file specific global attributes
        encoding (str): Encoding format of the file (default: Windows-1252)
        filename_convention (str): File name convention to extract attributes.
            Should be a regex expression.

    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    return parse_odf(
        path,
        vocabularies=vocabularies,
        global_attributes={**odf_global_attributes, **(global_attributes or {})},
        encoding=encoding,
        filename_convention=filename_convention,
    )
