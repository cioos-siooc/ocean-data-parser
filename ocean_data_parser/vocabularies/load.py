import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import requests

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


def dfo_platforms() -> pd.DataFrame:
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


def nerc_c17_to_platform(identifier):
    response = requests.get(
        f"https://vocab.nerc.ac.uk/collection/C17/current/{identifier}/?_profile=nvs&_mediatype=application/ld+json"
    )
    response.raise_for_status()
    data = response.json()
    # C17 definition is a json data
    platform_attributes = json.loads(data["definition"])
