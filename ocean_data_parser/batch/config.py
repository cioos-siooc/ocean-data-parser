import logging
from pathlib import Path
from collections.abc import Generator

import yaml
import pandas as pd

from ocean_data_parser.geo import read_geojson

MODULE_PATH = Path(__file__).parent
DEFAULT_CONFIG_PATH = MODULE_PATH / "default-batch-config.yaml"

logger = logging.getLogger(__name__)

def _get_paths(paths: str) -> list:
    if "*" in paths:
        path, glob_expr = paths.split("*", 1)
        glob_expr = f"*{glob_expr}"
        return [Path(path).glob(glob_expr)]
    return [Path(paths)]

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

    # Sentry
    if config.get("sentry", {}).get("dsn"):
        import sentry_sdk
        from sentry_sdk.integrations.loguru import LoguruIntegration
        from sentry_sdk.integrations.loguru import LoggingLevels

        sentry_loguru = LoguruIntegration(
            level=getattr(
                LoggingLevels, config["sentry"].get("level", "INFO")
            ).value,  # Capture info and above as breadcrumbs
            event_level=getattr(
                LoggingLevels, config["sentry"].get("event_level", "WARNING")
            ).value,  # Send errors as events
        )

        logger.info("Connect to sentry: {}", sentry_loguru)
        sentry_sdk.init(config["sentry"]["dsn"], integrations=[sentry_loguru])

    # Load config components
    if config.get("file_specific_attributes_path"):
        logger.info("Load file specific attributes")
        config["file_specific_attributes"] = pd.read_csv(
            config["file_specific_attributes_path"]
        ).set_index("file")

    if config.get("global_attribute_mapping").get("path"):
        logger.info("Load global attribute mapping")
        config["globab_attribute_mapping"]["dataframe"] = pd.concat(
            [
                pd.read_csv(path)
                for path in _get_paths(config["global_attribute_mapping"]["path"])
            ]
        )
        missing_mapping_variables = [
            var not in config["globab_attribute_mapping"]["dataframe"]
            for var in config["globab_attribute_mapping"]["by_variables"]
        ]
        if any(missing_mapping_variables):
            raise KeyError(
                "Missing variables: %s from %s",
                config["globab_attribute_mapping"]["by_variables"][
                    missing_mapping_variables
                ],
                config["global_attribute_mapping"]["path"],
            )

    return config


# config = load_config(DEFAULT_CONFIG_PATH)
