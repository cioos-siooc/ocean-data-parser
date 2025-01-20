import json

import pandas as pd
import requests
from pathlib import Path
from loguru import logger
import click


@logger.catch(reraise=True)
def get_vocabulary(vocab: str) -> pd.DataFrame:
    """Retrieve NERC vocabulary full list."""
    local_file = Path(__file__).parent / f"nerc_{vocab}_vocabulary.csv"
    if local_file.exists():
        return pd.read_csv(local_file)

    url = f"http://vocab.nerc.ac.uk/collection/{vocab}/current/?_profile=dd&_mediatype=application/json"
    logger.info("Load vocabulary: {}", url)
    df = pd.read_json(url)
    df["sdn_parameter_urn"] = f"SDN:{vocab.upper()}" + df["uri"].str.extract(".*/([^/]*)/$")
    logger.info("Save vocabulary: {}", local_file)
    return df


def get_vocabulary_term(vocab: str, id: str) -> dict:
    url = f"http://vocab.nerc.ac.uk/collection/{vocab}/current/{id}/?_profile=nvs&_mediatype=application/ld+json"
    logger.info("Load vocabulary term: {}", url)
    with requests.get(url) as response:
        return response.json()


def get_platform_vocabulary(id: str) -> dict:
    result = get_vocabulary_term("C17", id)
    # Parse the json data in the definition field
    attrs = json.loads(result["skos:definition"]["@value"])["node"]
    return {
        "platform_name": result["skos:prefLabel"]["@value"],
        "platform_type": attrs.get("platformclass"),
        "country_of_origin": attrs.get("country"),
        "platform_owner": attrs.get("title"),
        "platform_id": id,
        "ices_platform_code": id,
        "wmo_platform_code": attrs.get("IMO"),
        "call_sign": attrs.get("callsign"),
        "sdn_platform_urn": result["dc:identifier"],
    }


@click.command()
@click.option(
    "--output_dir",
    default=Path(__file__).parent,
    type=click.Path(exists=False),
    help="Output directory path.",
)
def update_package_reference_vocabularies(output_dir: Path):
    """Update the package reference vocabularies."""
    if not output_dir:
        output_dir = Path(__file__).parent
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    get_vocabulary("P01").to_csv(output_dir / "nerc_P01_vocabulary.csv", index=False)
    get_vocabulary("P06").to_csv(output_dir / "nerc_P06_vocabulary.csv")


if __name__ == "__main__":
    update_package_reference_vocabularies()
