import pandas as pd
import pytest

from ocean_data_parser.metadata import nerc
from ocean_data_parser.vocabularies import load

nerc_vocabulary_test = pytest.mark.skipif("not config.getoption('nerc_vocab')")

platforms_vocab = load.dfo_platforms()
platforms_vocab["wmo_platform_code"] = platforms_vocab["wmo_platform_code"].apply(
    lambda x: int(x) if x else x
)


def test_amundsen_vocabulary_load():
    vocab = load.amundsen_vocabulary()
    assert vocab
    assert isinstance(vocab, dict)


def test_seabird_vocabulary_load():
    vocab = load.seabird_vocabulary()
    assert vocab
    assert isinstance(vocab, dict)


class TestPlatformVocabulary:
    def test_dfo_platform_load(self):
        assert isinstance(platforms_vocab, pd.DataFrame)
        assert not platforms_vocab.empty
        assert "platform_name" in platforms_vocab.columns
        assert "02" in platforms_vocab["dfo_nafc_platform_code"].values

    @pytest.mark.parametrize(
        "column",
        [
            "platform_name",
            "platform_id",
            "ices_platform_code",
            "sdn_platform_urn",
            "dfo_nafc_platform_code",
            "dfo_nafc_platform_name",
        ],
    )
    def test_platform_unique_idendifiers(self, column):
        duplicated = platforms_vocab.query(
            f"{column}.notna() and {column}.duplicated(keep=False)"
        )
        assert duplicated.empty, f"{len(duplicated)} duplicated {column=} found in platform vocabulary: {duplicated[['platform_name',column]]}"

    def test_sdn_urm_prefix(self):
        sdn_prefix = platforms_vocab.query(
            "sdn_platform_urn.str.startswith('SDN:C17::') == False and sdn_platform_urn.notna()"
        )
        assert sdn_prefix.empty, f"SDN URN prefix not found in platform vocabulary: {sdn_prefix[['platform_name','sdn_platform_urn']]}"

    def test_matching_id_ices_sdn_codes(self):
        mismatched_codes = platforms_vocab.query(
            "ices_platform_code != platform_id and (ices_platform_code.notna() or platform_id.notna())"
        )
        assert mismatched_codes.empty, f"mismatched codes found: {mismatched_codes[['platform_name','platform_id','ices_platform_code','sdn_platform_urn']]}"
        mismatched_codes = platforms_vocab.query(
            "sdn_platform_urn.str.replace('SDN:C17::','') != platform_id and (sdn_platform_urn.notna() or platform_id.notna())"
        )
        assert mismatched_codes.empty, f"mismatched codes found: {mismatched_codes[['platform_name','platform_id','ices_platform_code','sdn_platform_urn']]}"

    @nerc_vocabulary_test
    @pytest.mark.parametrize(
        "id",
        platforms_vocab["platform_id"].dropna().values,
    )
    def test_nerc_c17_to_platform(self, id):
        attrs = [
            "platform_name",
            "platform_type",
            "country_of_origin",
            "platform_id",
            "ices_platform_code",
            "wmo_platform_code",
            "call_sign",
            "sdn_platform_urn",
        ]

        nerc_platform = nerc.get_platform_vocabulary(id)
        nerc_platform = {
            attr: nerc_platform.get(attr)
            for attr in attrs
            if nerc_platform.get(attr) is not None
        }
        local_platform = (
            platforms_vocab.query(f"platform_id == '{id}'")[attrs]
            .iloc[0]
            .dropna()
            .to_dict()
        )
        assert nerc_platform == local_platform


def test_dfo_odf_vocabulary_load():
    vocab = load.dfo_odf_vocabulary()
    assert isinstance(vocab, pd.DataFrame)
    assert not vocab.empty


def test_dfo_nafc_pfile_vocabulary_load():
    vocab = load.dfo_nafc_p_file_vocabulary()
    assert isinstance(vocab, pd.DataFrame)
    assert not vocab.empty
