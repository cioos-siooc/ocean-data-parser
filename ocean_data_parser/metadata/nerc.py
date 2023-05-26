import json

import pandas as pd
import requests


def get_vocabulary(vocab: str) -> pd.DataFrame:
    """Retrieve NERC vocabulary full list"""
    df = pd.read_json(
        f"http://vocab.nerc.ac.uk/collection/{vocab.upper()}/current/?_profile=dd&_mediatype=application/json"
    )
    df["sdn_parameter_urn"] = f"SDN:{vocab.upper()}" + df["uri"].str.extract(
        ".*/([^/]*)/$"
    )
    return df


def get_vocabulary_term(vocab: str, id: str) -> dict:
    url = f"http://vocab.nerc.ac.uk/collection/{vocab}/current/{id}/?_profile=nvs&_mediatype=application/ld+json"
    with requests.get(url) as response:
        json_text = response.text
    return json.loads(json_text)
