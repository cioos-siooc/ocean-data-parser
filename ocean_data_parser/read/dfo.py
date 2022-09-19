from ocean_data_parser.read.odf.process import (
    parse_odf,
    read_config,
)


def bio_odf(path, config=None):
    """Make sure to use only the Bedford Institute specific vocabulary"""

    if config is None:
        config = read_config()

    config["organisationVocabulary"] = ["BIO", "GF3"]
    return parse_odf(path, config=config)


def mli_odf(path, config=None):
    """Make sure to use only the Maurice Lamontagne Institute specific vocabulary"""
    if config is None:
        config = read_config()

    config["organisationVocabulary"] = ["MLI", "GF3"]
    return parse_odf(path, config=config)
