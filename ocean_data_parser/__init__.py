import re
from pathlib import Path
from ocean_data_parser._version import __version__

PARSERS = re.findall('parser = \"(.*)\"',Path('ocean_data_parser/read.py').read_text())