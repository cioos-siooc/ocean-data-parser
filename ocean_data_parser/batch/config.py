import logging
from pathlib import Path
from collections.abc import Generator

import yaml
import pandas as pd

from ocean_data_parser.geo import read_geojson

MODULE_PATH = Path(__file__).parent
DEFAULT_CONFIG_PATH = MODULE_PATH / "default-batch-config.yaml"

logger = logging.getLogger(__name__)


def glob(paths: str) -> Generator[Path]:
    """Create a generator of paths from a glob path expression

    Args:
        paths (str): glob type apth

    Yields:
        Generator[Path]: generator of Path objects
    """
    paths = Path(paths)
    anchor = paths.anchor
    return Path(anchor).glob(str(paths.relative_to(anchor)))


def load_config(config_path: str = None, encoding="UTF-8"):
    """Load YAML configuration file, if not provided load default configuration."""
    # Get default config if no file provided
    if config_path is None:
        config_path = Path(__file__).parent / "default-batch-config.yaml"

    with open(config_path, "r", encoding=encoding) as file:
        config = yaml.load(file, Loader=yaml.SafeLoader)

    # Load geojson regions
    if config.get("reference_regions") and config["reference_regions"].get("path"):
        for path in glob(config["reference_regions"]["path"]):
            config["reference_regions"]["regions"].update(read_geojson(path))

    # Load reference stations
    if config.get("reference_stations") and config["reference_stations"].get("path"):
        config["reference_stations"]["stations"] = pd.concat(
            [
                pd.read_csv(path)
                for path in glob(config["reference_stations"]["path"])
                if path
            ]
        )

    return config


config = load_config(DEFAULT_CONFIG_PATH)
