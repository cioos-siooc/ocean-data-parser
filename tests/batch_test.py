import logging
import unittest
import os

import ocean_data_parser.batch as batch

PACKAGE_PATH = os.path.dirname(__file__)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class BatchTests(unittest.TestCase):
    def test_txt_parser(self):
        config = batch.load_config()
        config["input"] = os.path.join(
            PACKAGE_PATH, "tests/parser_test_files/onset/**.*.csv"
        )
        config["input"] = os.path.join(
            PACKAGE_PATH, "tests/parser_test_files/onset/**.*.csv"
        )
