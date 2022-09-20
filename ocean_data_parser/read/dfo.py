from typing import Union
from ocean_data_parser.read.odf.process import (
    parse_odf,
    read_config,
)


def bio_odf(path: str, config: Union[str, dict] = None):
    """Bedford Institute of Ocean ODF format parser
    Args:
        path (str): Path to the odf file to parse
        config (dict): Configuration parameters used to parse the odf file.
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    if config is None:
        config = read_config()

    config["organisationVocabulary"] = ["BIO", "GF3"]
    return parse_odf(path, config=config)


def mli_odf(path: str, config: Union[str, dict] = None):
    """Maurice Lamontagne Institute ODF format parser
    Args:
        path (str): Path to the odf file to parse
        config (dict): Configuration parameters used to parse the odf file.
    Returns:
        dataset (xarray dataset): Parsed xarray dataset
    """
    if config is None:
        config = read_config()

    config["organisationVocabulary"] = ["MLI", "GF3"]
    return parse_odf(path, config=config)
