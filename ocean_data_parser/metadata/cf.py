import pandas as pd
from pathlib import Path
import click
from loguru import logger

DEFAULT_STANDARD_NAMES_VERSION = 81


def get_standard_names(version=DEFAULT_STANDARD_NAMES_VERSION) -> pd.DataFrame:
    """Load CF convention Standard Names table."""

    def _get_value(column):
        return standard_names[column].dropna().unique()[0]

    unique_value_columns = ["version_number", "last_modified", "institution", "contact"]

    local_file = Path(__file__).parent / "cf_standard_names_v{version}.csv"
    if local_file.exists():
        return pd.read_csv(local_file)

    url = f"https://cfconventions.org/Data/cf-standard-names/{version}/src/cf-standard-name-table.xml"
    logger.info("Load cf-standard-names: {}", url)
    standard_names = pd.read_xml(
        f"https://cfconventions.org/Data/cf-standard-names/{version}/src/cf-standard-name-table.xml"
    )
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
@click.option(
    "--output",
    default=None,
    type=click.Path(exists=False),
    help="Output CSV file path.",
)
def save_standard_names(version, output: Path):
    """Save CF convention Standard Names table to a CSV file."""
    if output is None:
        output = Path(__file__).parent / f"cf_standard_names_v{version}.csv"
    standard_names = get_standard_names(version)
    if output.suffix != ".csv":
        output = output.with_suffix(".csv")
    standard_names.to_csv(output, index=False)


if __name__ == "__main__":
    save_standard_names()
