import logging

import pandas as pd
import pytest

from ocean_data_parser.metadata import cf, nerc, pdc

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestPDCMetadatas:
    def test_fgdc_to_acdd_on_profiles(self):
        ccins = [80, 12713, 12715, 12518]
        for ccin in ccins:
            logger.debug("ccin=%s", ccin)
            fgdc_metadata_url = (
                f"https://www.polardata.ca/pdcsearch/xml/fgdc/{ccin}_fgdc.xml"
            )
            pdc.fgdc_to_acdd(fgdc_metadata_url)


class TestCFStandardName:
    def test_get_standard_names_default_version(self):
        standard_names = cf.get_standard_names()
        assert not standard_names.empty, "Failed to retrieve any standard_names"
        assert standard_names.attrs, "standard_name.attrs is empty"

    def test_get_standard_names_v70(self):
        standard_names = cf.get_standard_names(version=70)
        assert not standard_names.empty, "Failed to retrieve any standard_names"
        assert standard_names.attrs, "standard_name.attrs is empty"
        assert standard_names.attrs["version_number"] == 70, (
            "standard_name.attrs.version_number doesn't match v70"
        )


class TestNERCVocabularies:
    def test_get_nerc_p01_vocabulary(self):
        p01 = nerc.get_vocabulary("P01")
        assert isinstance(p01, pd.DataFrame), (
            "nerc vocabulary retrieved isn't a pandas DataFrame"
        )
        assert not p01.empty, "nerc P01 vocabulary retrieved is empty"

    def test_get_nerc_p01_terms(self):
        ids = ["TEMPPR01"]
        for id in ids:
            id_info = nerc.get_vocabulary_term("P01", id)
            assert id_info is not None, "term vocabulary retrieved is None"
            assert isinstance(id_info, dict), "term vocabulary isn't a dictionary"

    @pytest.mark.parametrize(("id", "expected"), [("18DL", "Amundsen")])
    def test_retrieve_platform(self, id, expected):
        platform = nerc.get_platform_vocabulary(id)
        assert platform["platform_name"] == expected, (
            f"platform name {platform['platform_name']} doesn't match expected {expected}"
        )
