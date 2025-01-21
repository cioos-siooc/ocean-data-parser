import json

import pandas as pd
import requests
from pathlib import Path
from loguru import logger
import click


class Nerc:
    def __init__(self, base_url: str = "http://vocab.nerc.ac.uk/collection/"):
        self.base_url = base_url

    def download_vocabulary(
        self,
        vocabulary: str,
        version: str = "current",
        cache_dir: Path = Path(__file__).parent,
        overwrite: bool = True,
    ) -> pd.DataFrame:
        """Download NERC vocabulary full list."""
        cached_file = cache_dir / f"nerc_{vocabulary}_vocabulary.json"
        if cached_file.exists() and not overwrite:
            return cached_file
        url = f"{self.base_url}/{vocabulary}/{version}/?_profile=dd&_mediatype=application/json"
        logger.info("Dowonload vocabulary: {}", url)
        # Save file locally
        with requests.get(url) as response:
            response.raise_for_status()
            with open(cached_file, "wb") as f:
                f.write(response.content)

        return cached_file

    @logger.catch(reraise=True)
    def get_vocabulary(
        self, vocabulary: str, version: str = "current", ignore_cache: bool = False
    ) -> pd.DataFrame:
        """Retrieve NERC vocabulary full list."""
        logger.info("Load vocabulary: {}", vocabulary)
        json_file = self.download_vocabulary(
            vocabulary, version=version, overwrite=ignore_cache
        )
        logger.info("Load vocabulary from file: {}", json_file)
        return pd.read_json(json_file)

    def get_vocabulary_term(
        self, vocabulary: str, id: str, version: str = "current"
    ) -> dict:
        url = f"{self.base_url}/{vocabulary}/{version}/{id}/?_profile=nvs&_mediatype=application/ld+json"
        logger.info("Load vocabulary term: {}", url)
        with requests.get(url) as response:
            response.raise_for_status()
            return response.json()

    def get_urn_from_uri(self, uri: str) -> str:
        _, vocabulary, version, id, _ = uri.rsplit("/", 4)
        return f"{vocabulary}:{version}::{id}"

    def get_p01_vocabulary(self) -> pd.DataFrame:
        df = self.get_vocabulary("P01")
        df["sdn_parameter_urn"] = df["uri"].apply(self.get_urn_from_uri)
        return df.rename(columns={"prefLabel": "sdn_parameter_name"})

    def get_p06_vocabulary(self) -> pd.DataFrame:
        df = self.get_vocabulary("P06")
        df["sdn_uom_urn"] = df["uri"].apply(self.get_urn_from_uri)
        return df.rename(columns={"prefLabel": "sdn_uom_name"})

    def get_platform_vocabulary(self, id: str) -> dict:
        result = self.get_vocabulary_term("C17", id)
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
    nerc = Nerc()
    nerc.get_p01_vocabulary()
    nerc.get_p06_vocabulary()


if __name__ == "__main__":
    update_package_reference_vocabularies()
