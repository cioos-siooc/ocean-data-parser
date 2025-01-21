import pandas as pd
import pytest
import re

from ocean_data_parser.metadata import nerc, cf
from ocean_data_parser.vocabularies import load

nerc_vocabulary_test = pytest.mark.skipif("not config.getoption('nerc_vocab')")

platforms_vocab = load.dfo_platforms()
platforms_vocab["wmo_platform_code"] = platforms_vocab["wmo_platform_code"].apply(
    lambda x: int(x) if x else x
)


standard_names = cf.get_standard_names()
nerc_p01 = nerc.Nerc().get_p01_vocabulary()
nerc_p06 = nerc.Nerc().get_p06_vocabulary()


class TestVocabularyLoad:
    def test_amundsen_vocabulary_load():
        vocab = load.amundsen_vocabulary()
        assert vocab
        assert isinstance(vocab, dict)

    def test_seabird_vocabulary_load():
        vocab = load.seabird_vocabulary()
        assert vocab
        assert isinstance(vocab, dict)

    def test_dfo_odf_vocabulary_load():
        vocab = load.dfo_odf_vocabulary()
        assert isinstance(vocab, pd.DataFrame)
        assert not vocab.empty

    def test_dfo_nafc_pfile_vocabulary_load():
        vocab = load.dfo_nafc_p_file_vocabulary()
        assert isinstance(vocab, pd.DataFrame)
        assert not vocab.empty


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
        assert duplicated.empty, (
            f"{len(duplicated)} duplicated {column=} found in platform vocabulary: {duplicated[['platform_name', column]]}"
        )

    def test_sdn_urm_prefix(self):
        sdn_prefix = platforms_vocab.query(
            "sdn_platform_urn.str.startswith('SDN:C17::') == False and sdn_platform_urn.notna()"
        )
        assert sdn_prefix.empty, (
            f"SDN URN prefix not found in platform vocabulary: {sdn_prefix[['platform_name', 'sdn_platform_urn']]}"
        )

    def test_matching_id_ices_sdn_codes(self):
        mismatched_codes = platforms_vocab.query(
            "ices_platform_code != platform_id and (ices_platform_code.notna() or platform_id.notna())"
        )
        assert mismatched_codes.empty, (
            f"mismatched codes found: {mismatched_codes[['platform_name', 'platform_id', 'ices_platform_code', 'sdn_platform_urn']]}"
        )
        mismatched_codes = platforms_vocab.query(
            "sdn_platform_urn.str.replace('SDN:C17::','') != platform_id and (sdn_platform_urn.notna() or platform_id.notna())"
        )
        assert mismatched_codes.empty, (
            f"mismatched codes found: {mismatched_codes[['platform_name', 'platform_id', 'ices_platform_code', 'sdn_platform_urn']]}"
        )

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


vocabularies = [
    load.amundsen_vocabulary_df,
    load.seabird_vocabulary_df,
    load.dfo_odf_vocabulary,
    load.dfo_nafc_p_file_vocabulary,
]
ACCEPTED_URN_VARIANCE = [
    "XX",
    "X",
    "1",
    "01",
]


def is_valid_urn(urn, urns):
    if pd.isna(urn):
        return True
    if urn in urns:
        return True
    if not re.search(r"\d+$", urn):
        return False
    for variance in ACCEPTED_URN_VARIANCE:
        if re.sub(r"\d+$", variance, urn) in urns:
            return True
    return False


def get_matching_urn(urn: str, name: str, urns: list, names: list):
    if pd.isna(urn):
        return
    if urn in urns:
        return urn
    if not re.search(r"\d+$", urn):
        return
    for variance in ACCEPTED_URN_VARIANCE:
        if re.sub(r"\d+$", variance, urn) not in urns:
            continue
        # test if name is the same as the one in the reference
        if names[urns.index(re.sub(r"\d+$", variance, urn))] == name:
            return re.sub(r"\d+$", variance, urn)

    return


@pytest.mark.parametrize("load_vocabulary", vocabularies)
class TestVocabularies:
    @pytest.fixture(autouse=True)
    def vocabulary(self, load_vocabulary):
        return load_vocabulary()

    def test_standard_names(self, vocabulary):
        """Test that the standard_name column is correctly formatted."""
        assert "standard_name" in vocabulary.columns
        unknown_standard_names = vocabulary.query(
            "standard_name.notna() and standard_name not in @standard_names['id']"
        )
        assert unknown_standard_names.empty, (
            f"{len(unknown_standard_names)} unknown standard names found: {unknown_standard_names.to_dict(orient='records')}"
        )

    def test_sdn_parameter_urn(self, vocabulary):
        """Test that the sdn_parameter_urn column is correctly formatted."""
        if "sdn_parameter_urn" not in vocabulary.columns:
            return

        not_p01 = ~(
            vocabulary["sdn_parameter_urn"].isna()
            | vocabulary["sdn_parameter_urn"].str.startswith("SDN:P01::")
        )
        assert not not_p01.any(), (
            f"Parameter URNs do not start with 'SDN:P01::': {vocabulary['sdn_parameter_urn'][not_p01]}"
        )

        valid_p01_urn = vocabulary["sdn_parameter_urn"].apply(
            lambda urn: is_valid_urn(urn, nerc_p01["sdn_parameter_urn"].to_list())
        )
        unknown_urns = vocabulary.loc[~valid_p01_urn]
        assert valid_p01_urn.all(), (
            f"{len(unknown_urns)} unknown parameter URNs found [{unknown_urns['sdn_parameter_urn'].unique()}]: {unknown_urns.to_dict(orient='records')}"
        )
        # convert vocabulary sdn_parameter_urn et sdn_parameter_name to a dictionary
        vocabulary["matched_urn"] = vocabulary.apply(
            lambda urn: get_matching_urn(
                urn["sdn_parameter_urn"],
                urn["sdn_parameter_name"],
                nerc_p01["sdn_parameter_urn"].to_list(),
                nerc_p01["sdn_parameter_name"].to_list(),
            ),
            axis=1,
        )

        comparison = vocabulary.merge(
            nerc_p01[["sdn_parameter_urn", "sdn_parameter_name"]],
            right_on=["sdn_parameter_name", "sdn_parameter_urn"],
            left_on=["sdn_parameter_name", "matched_urn"],
            how="left",
            indicator=True,
            suffixes=("", "_reference"),
        ).dropna(subset=["sdn_parameter_name", "sdn_parameter_urn"], how="all")
        mismatches = comparison[comparison["_merge"] == "left_only"]
        assert mismatches.empty, (
            f"Bad {len(mismatches)} mismatches found {mismatches['sdn_parameter_urn'].unique()}: {mismatches.to_json(orient='index')}"
        )

    def test_sdn_parameter_name(self, vocabulary):
        """Test that the sdn_parameter_name column is correctly formatted.

        We accept variances of existing URNs ending with XX, X, 1 or 01.
        As long as this variance is numerical variance only.
        """
        if "sdn_parameter_name" not in vocabulary.columns:
            return
        unknown_names = vocabulary.query(
            "sdn_parameter_name.notna() and sdn_parameter_name not in @nerc_p01['sdn_parameter_name']"
        )
        assert unknown_names.empty, (
            f"{len(unknown_names)} unknown parameter names found [{unknown_names['sdn_parameter_name'].unique()}]: {unknown_names.to_json(orient='index')}"
        )

    def test_sdn_uom_urn(self, vocabulary):
        """Test that the sdn_uom_urn column is correctly formatted."""
        if "sdn_uom_urn" not in vocabulary.columns:
            return
        not_p06 = ~(
            vocabulary["sdn_uom_urn"].isna()
            | vocabulary["sdn_uom_urn"].str.startswith("SDN:P06::")
        )
        assert not not_p06.any(), (
            f"UOM URNs do not start with 'SDN:P06::': {vocabulary['sdn_uom_urn'][not_p06]}"
        )
        unknown_urns = vocabulary.query(
            "sdn_uom_urn.notna() and sdn_uom_urn not in @nerc_p06['sdn_uom_urn']"
        )
        assert unknown_urns.empty, (
            f"{len(unknown_urns)} unknown UOM URNs found: {unknown_urns.to_dict(orient='records')}"
        )

    # @pytest.mark.parametrize("vocabulary", vocabularies)
    def test_sdn_uom_name(self, vocabulary):
        """Test that the sdn_uom_name column is correctly formatted."""
        if "sdn_uom_name" not in vocabulary.columns:
            return
        unknown_names = vocabulary.query(
            "sdn_uom_name.notna() and sdn_uom_name not in @nerc_p06['sdn_uom_name']"
        )
        assert unknown_names.empty, (
            f"{len(unknown_names)} unknown UOM names found [{set(unknown_names.sdn_uom_name.to_list())}]: {unknown_names.to_records()}"
        )

        comparison = vocabulary.merge(
            nerc_p06[["sdn_uom_urn", "sdn_uom_name"]],
            on=["sdn_uom_name", "sdn_uom_urn"],
            how="left",
            indicator=True,
        ).dropna(subset=["sdn_uom_name", "sdn_uom_urn"], how="all")
        mismatches = comparison[comparison["_merge"] == "left_only"]
        assert mismatches.empty, (
            f"Found mismatched entries: {mismatches.to_dict(orient='records')}"
        )
