import re
from pathlib import Path

from ocean_data_parser._version import __version__

PARSERS = re.findall('parser = "(.*)"', (Path(__file__).parent / "read.py").read_text())
