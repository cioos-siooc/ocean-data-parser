"""Main module for ocean_data_parser package."""

import re
from pathlib import Path

PARSERS = re.findall('parser = "(.*)"', (Path(__file__).parent / "read.py").read_text())
__version__ = "0.7.0"
