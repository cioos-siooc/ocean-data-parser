import logging
import unittest

from ocean_data_parser import metadata

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
class PDCMetadataTests(unittest.TestCase):
    def test_fgdc_to_acdd_on_profiles(self):
        ccins = [80, 12713, 12715, 12518]
        for ccin in ccins:
            logger.debug("ccin=%s",ccin)
            fgdc_metadata_url = (
                f"https://www.polardata.ca/pdcsearch/xml/fgdc/{ccin}_fgdc.xml"
            )
            metadata.pdc.fgdc_to_acdd(fgdc_metadata_url)
            