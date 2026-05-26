"""This is the main module of the package. It contains the version number and the list of parsers available in the package."""

import re
from pathlib import Path

PARSERS = re.findall(
    'parser = "(.*)"', (Path(__file__).parent / "read.py").read_text()
) + ["dfo.odf.odf", "dfo.odf.as_qo_odf"]
__version__ = "0.7.0"
