import re
from pathlib import Path


PARSERS = re.findall('parser = "(.*)"', (Path(__file__).parent / "read.py").read_text())
