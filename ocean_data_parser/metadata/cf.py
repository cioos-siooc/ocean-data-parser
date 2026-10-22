from pathlib import Path

import click
import pandas as pd
import requests
from loguru import logger

DEFAULT_STANDARD_NAMES_VERSION = 95
CACHE_DIR = Path(__file__).parent


def _cached_file(version) -> Path:
    return CACHE_DIR / f"cf_standard_names_v{version}.xml"


def _url(version) -> str:
    return f"https://cfconventions.org/Data/cf-standard-names/{version}/src/cf-standard-name-table.xml"


def get_standard_names(version=DEFAULT_STANDARD_NAMES_VERSION) -> pd.DataFrame:
    """Load CF convention Standard Names table.

    The table is read from the copy stored in the package if available,
    otherwise it is retrieved from cfconventions.org.
    """

    def _get_value(column):
        return standard_names[column].dropna().unique()[0]

    unique_value_columns = ["version_number", "last_modified", "institution", "contact"]

    source = _cached_file(version)
    if not source.exists():
        source = _url(version)
    logger.info("Load cf-standard-names: {}", source)
    standard_names = pd.read_xml(source)
    standard_names.attrs = {item: _get_value(item) for item in unique_value_columns}
    return (
        standard_names.drop(index=[0, 1, 2, 3])
        .drop(columns=unique_value_columns)
        .reset_index(drop=True)
    )


@click.command()
@click.option(
    "--version",
    default=DEFAULT_STANDARD_NAMES_VERSION,
    help="CF convention Standard Names table version number.",
)
def update_standard_names(version):
    """Store CF convention Standard Names table in the package.

    Only one version is kept: any other stored version is deleted.
    """
    url = _url(version)
    logger.info("Download cf-standard-names: {}", url)
    with requests.get(url) as response:
        response.raise_for_status()
        cached_file = _cached_file(version)
        cached_file.write_bytes(response.content)

    for file in CACHE_DIR.glob("cf_standard_names_v*"):
        if file != cached_file:
            logger.info("Delete previous cf-standard-names: {}", file)
            file.unlink()


if __name__ == "__main__":
    update_standard_names()
