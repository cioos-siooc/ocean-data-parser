import unittest
from ocean_data_parser.metadata import pdc
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
class PDC_Metadata_Tests(unittest.TestCase):
    def test_fgdc_to_acdd_on_profiles(self):
        ccins = [12477, 12715, 12518]
        for ccin in ccins:
            fgdc_metadata_url = (
                f"https://www.polardata.ca/pdcsearch/xml/fgdc/{ccin}_fgdc.xml"
            )
            pdc.fgdc_to_acdd(fgdc_metadata_url)
            