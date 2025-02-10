import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

VOCABULARIES_DIRECTORY = Path(__file__).parent


def amundsen_vocabulary_df(instrument_vocabulary:str=None) -> pd.DataFrame:
    """Retrieve Amundsen Vocabulary as DataFrame.

    Args:
        instrument_vocabulary (str): Instrument vocabulary to load. Defaults to Rosette.
    
    """
    vocabulary = amundsen_vocabulary(instrument_vocabulary)
    vocab = []
    for name, attrs in vocabulary.items():
        for attr in attrs:
            vocab += [{"variable_name": name, **attr}]
    return pd.DataFrame(vocab)


def amundsen_vocabulary(instrument_vocabulary: str = None) -> dict:
    """Load Amundsen Vocabulary.

    Args:
        instrument_vocabulary (str): Instrument vocabulary to load. Defaults to None.

    Returns:
        dict: Amundsen Vocabulary
    """
    if not instrument_vocabulary or instrument_vocabulary == "rosette":
        vocabulary_file = VOCABULARIES_DIRECTORY / "amundsen_rosette_vocabulary.json"
    else:
        vocabulary_file = VOCABULARIES_DIRECTORY / "amundsen_other_vocabulary.json"

    with open(vocabulary_file) as file:
        with open(vocabulary_file, encoding="UTF-8") as file:
            vocab = json.load(file)

    vocab.pop("VARIABLE_NAME")
    if not instrument_vocabulary or instrument_vocabulary == "rosette":
        return vocab

    sub_vocab = {}
    for name, items in vocab.items():
        for item in items:
            if item.get("file_type") == instrument_vocabulary:
                if name not in sub_vocab:
                    sub_vocab[name] = []
                sub_vocab[name].append(item)
    return sub_vocab


def seabird_vocabulary_df() -> pd.DataFrame:
    """Retrieve Seabird Vocabulary as DataFrame."""
    vocabulary = seabird_vocabulary()
    vocab = []
    for name, attrs in vocabulary.items():
        vocab += [{"variable_name": name, **attrs}]
    return pd.DataFrame(vocab)


def seabird_vocabulary() -> dict:
    """Load Seabird Vocabulary."""
    with open(
        VOCABULARIES_DIRECTORY / "seabird_vocabulary.json", encoding="UTF-8"
    ) as file:
        vocabulary = json.load(file)
    # Make it non case sensitive by lowering all keys
    vocabulary = {key.lower(): attrs for key, attrs in vocabulary.items()}
    return vocabulary


def dfo_platforms() -> pd.DataFrame:
    """Retrieve DFO Platforms vocabulary."""
    df = (
        pd.read_csv(
            VOCABULARIES_DIRECTORY / "dfo_platforms.csv",
            dtype={
                "wmo_platform_code": "string",
                "dfo_newfoundland_ship_code": "string",
            },
        )
        .astype(object)
        .replace({pd.NA: None})
    )
    df["dfo_nafc_platform_code"] = df["dfo_nafc_platform_code"].apply(
        lambda x: f"{int(x):02g}" if re.match(r"\s*\d+", x or "") else x
    )
    return df


def dfo_ios_vocabulary() -> pd.DataFrame:
    return pd.read_csv(VOCABULARIES_DIRECTORY / "dfo_ios_vocabulary.csv")


def as_qo_odf_vocabulary() -> pd.DataFrame:
    """Transform AS QO vocabulary to ODF vocabulary."""
    df_vocab = (
        amundsen_vocabulary_df()
        .rename(columns={"variable_name": "name"})
        .assign(Vocabulary="AS_QO")
    )

    return df_vocab


def dfo_odf_vocabulary() -> pd.DataFrame:
    """Combine DFO ODF and AS QO vocabularies."""
    return (
        pd.concat(
            [
                pd.read_csv(VOCABULARIES_DIRECTORY / "dfo_odf_vocabulary.csv"),
                as_qo_odf_vocabulary(),
            ]
        )
        .fillna(np.nan)
        .replace({np.nan: None})
    )


def dfo_nafc_p_file_vocabulary() -> pd.DataFrame:
    return pd.read_csv(
        VOCABULARIES_DIRECTORY / "dfo_nafc_p_files_vocabulary.csv"
    ).replace({"variable_name": {np.nan: None}})
