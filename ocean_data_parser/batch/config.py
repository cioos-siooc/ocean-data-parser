import logging
import os
import re
from glob import glob
from importlib import import_module
from pathlib import Path

import yaml

MODULE_PATH = Path(__file__).parent
DEFAULT_CONFIG_PATH = MODULE_PATH / "default-batch-config.yaml"

logger = logging.getLogger(__name__)


def load_config(config_path: str = None, encoding="UTF-8"):
    """Load YAML configuration file, if not provided load default configuration."""
    # Get default config if no file provided
    if config_path is None:
        config_path = os.path.join(
            os.path.dirname(__file__), "default-batch-config.yaml"
        )

    with open(config_path, "r", encoding=encoding) as file:
        config = yaml.load(file, Loader=yaml.SafeLoader)

    return config


config = load_config(DEFAULT_CONFIG_PATH)
