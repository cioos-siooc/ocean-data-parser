"""This package contains all the parsers related to the
Fisheries and Ocean Canada standard formats.

Modules:
    - __init__.py: Initializes the parsers package.
    - ios.py: Contains functions to parse the DFO-IOS office file formats.
    - nafc.py: Contains functions to parse DFO-NAFC office file formats.
    - odf.py: Contains functions to parse the ODF file format used at
        the DFO-BIO and DFO-IML offices.

"""

__all__ = ["ios", "nafc", "odf"]
from . import ios, nafc, odf
