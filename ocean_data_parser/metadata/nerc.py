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
        return response.json()


def get_platform_vocabulary(id: str) -> dict:
    result = get_vocabulary_term("C17", id)
    # Parse the json data in the definition field
    attrs = json.loads(result["definition"]["@value"])["node"]
    return {
        "platform_name": result["prefLabel"]["@value"],
        "platform_type": attrs["platformclass"],
        "country_of_origin": attrs["country"],
        "platform_owner": attrs["title"],
        "platform_id": id,
        "ices_platform_codes": id,
        "wmo_platform_code": attrs.get("IMO"),
        "call_sign": attrs.get("callsign"),
        "sdn_platform_urn": result["identifier"],
    }
