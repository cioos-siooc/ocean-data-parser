import json
import logging
import unittest
import os

import yaml
import ocean_data_parser.batch as batch

PACKAGE_PATH = __path__
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
with open(os.path.join(PACKAGE_PATH,"ocean_data_parser/batch/sample-batch-config.yaml"), encoding="utf-8") as f:
with open(
    os.path.join(PACKAGE_PATH, "ocean_data_parser/batch/sample-batch-config.yaml"),
    encoding="utf-8",
) as f:


class OnsetBatchTests(unittest.TestCase):
    def test_txt_parser(self):
        config = default_config
        config['input'] = os.path.join(PACKAGE_PATH,'tests/parser_test_files/onset/**.*.csv')
        config["input"] = os.path.join(
            PACKAGE_PATH, "tests/parser_test_files/onset/**.*.csv"
        )
