import json
from pathlib import Path
import pandas as pd
import numpy as np

VOCABULARIES_DIRECTORY = Path(__file__).parent


def amundsen_vocabulary() -> dict:
    with open(VOCABULARIES_DIRECTORY / "amundsen_vocabulary.json") as file:
        return json.load(file)


def seabird_vocabulary() -> dict:
    """Load Seabird Vocabulary"""
    with open(
        VOCABULARIES_DIRECTORY / "seabird_vocabulary.json", encoding="UTF-8"
    ) as file:
        vocabulary = json.load(file)
    # Make it non case sensitive by lowering all keys
    vocabulary = {key.lower(): attrs for key, attrs in vocabulary.items()}
    return vocabulary


def dfo_platforms(index="platform_name") -> pd.DataFrame:
    return pd.read_csv(
        VOCABULARIES_DIRECTORY / "dfo_platforms.csv",
        dtype={"wmo_platform_code": "string"},
    ).set_index(index)


def dfo_odf_vocabulary() -> pd.DataFrame:
    return (
        pd.read_csv(VOCABULARIES_DIRECTORY / "dfo_odf_vocabulary.csv")
        .fillna(np.nan)
        .replace({np.nan: None})
    )


def dfo_nafc_p_file_vocabulary() -> pd.DataFrame:
    return pd.read_csv(
        VOCABULARIES_DIRECTORY / "dfo_nafc_p_files_vocabulary.csv"
    ).replace({"variable_name": {np.nan: None}})
