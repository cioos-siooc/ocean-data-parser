import pandas as pd
import pytest

from ocean_data_parser.vocabularies import load


@pytest.fixture(scope="module")
def platforms_vocab():
    return load.dfo_platforms()


def test_amundsen_vocabulary_load():
    vocab = load.amundsen_vocabulary()
    assert vocab
    assert isinstance(vocab, dict)


def test_seabird_vocabulary_load():
    vocab = load.seabird_vocabulary()
    assert vocab
    assert isinstance(vocab, dict)


class TestPlatformVocabulary:
    def test_dfo_platform_load(self, platforms_vocab):
        assert isinstance(platforms_vocab, pd.DataFrame)
        assert not platforms_vocab.empty
        assert "platform_name" in platforms_vocab.columns
        assert "02" in platforms_vocab["dfo_nafc_platform_code"].values

    @pytest.mark.parametrize(
        "column",
        [
            "platform_name",
            "platform_id",
            "ices_platform_codes",
            "wmo_platform_code",
            "call_sign",
            "sdn_platform_urn",
            "dfo_nafc_platform_code",
            "dfo_nafc_platform_name",
        ],
    )
    def test_platform_unique_idendifiers(self, platforms_vocab, column):
        duplicated = platforms_vocab.query(
            f"{column}.notna() and {column}.duplicated(keep=False)"
        )
        assert (
            duplicated.empty
        ), f"{len(duplicated)} duplicated {column=} found in platform vocabulary: {duplicated[['platform_name',column]]}"

    def test_sdn_urm_prefix(self, platforms_vocab):
        sdn_prefix = platforms_vocab.query(
            "sdn_platform_urn.str.startswith('SDN:C17::') == False and sdn_platform_urn.notna()"
        )
        assert (
            sdn_prefix.empty
        ), f"SDN URN prefix not found in platform vocabulary: {sdn_prefix[['platform_name','sdn_platform_urn']]}"

    def test_matching_id_ices_sdn_codes(self, platforms_vocab):
        mismatched_codes = platforms_vocab.query(
            "ices_platform_codes != platform_id and (ices_platform_codes.notna() or platform_id.notna())"
        )
        assert (
            mismatched_codes.empty
        ), f"mismatched codes found: {mismatched_codes[['platform_name','platform_id','ices_platform_codes','sdn_platform_urn']]}"
        mismatched_codes = platforms_vocab.query(
            "sdn_platform_urn.str.replace('SDN:C17::','') != platform_id and (sdn_platform_urn.notna() or platform_id.notna())"
        )
        assert (
            mismatched_codes.empty
        ), f"mismatched codes found: {mismatched_codes[['platform_name','platform_id','ices_platform_codes','sdn_platform_urn']]}"


def test_dfo_odf_vocabulary_load():
    vocab = load.dfo_odf_vocabulary()
    assert isinstance(vocab, pd.DataFrame)
    assert not vocab.empty


def test_dfo_nafc_pfile_vocabulary_load():
    vocab = load.dfo_nafc_p_file_vocabulary()
    assert isinstance(vocab, pd.DataFrame)
    assert not vocab.empty
