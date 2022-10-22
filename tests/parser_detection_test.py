import logging
import unittest
from glob import glob

from ocean_data_parser.read.utils import detect_file_format

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class ParserDetectionTests(unittest.TestCase):
    def test_all_test_files(self):
        test_files = glob("tests/parsers_test_files/**/*.*", recursive=True)
        for file in test_files:
            if "nmea" in file or file.endswith("nc"):
                continue
            parser = detect_file_format(file)
            assert parser, f"Test file {file} doesn't match any parser"

    def test_amundsen(self):
        test_files = glob("tests/parsers_test_files/amundsen/**/*.*", recursive=True)
        for file in test_files:
            if file.endswith("nc"):
                continue
            parser = detect_file_format(file)
            assert (
                parser == "amundsen.int_format"
            ), f"Test file {file} doesn't match amundsen.int_format"

    def test_bio_odf(self):
        test_files = glob("tests/parsers_test_files/dfo/bio/**/*.*", recursive=True)
        for file in test_files:
            if file.endswith("nc"):
                continue
            parser = detect_file_format(file)
            assert (
                parser == "dfo.odf.bio_format"
            ), f"Test file {file} doesn't match dfo.odf.bio_format"
