import logging
import unittest
from pathlib import Path

from click.testing import CliRunner

from ocean_data_parser.compile.netcdf import compile, variables

PACKAGE_PATH = Path(__file__).parent
logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")
TEST_FILE_PATH = str(PACKAGE_PATH / "parser_test_files" / "dfo" / "bio")


class CompileNetcdfVariablesTests(unittest.TestCase):
    def test_compile_variables_default(self):
        variables(TEST_FILE_PATH, "**/*")

    def test_compile_variables_md_console(self):
        variables(TEST_FILE_PATH, "**/*", output_table=True)

    def test_compile_variables_md_csv(self):
        variables(TEST_FILE_PATH, "**/*", output_table="test_compile_table.csv")

    def test_compile_variables_table_markdown(self):
        variables(TEST_FILE_PATH, "**/*", output_table="test_compile_table.md")

    def test_compile_variables_xml_console(self):
        variables(TEST_FILE_PATH, "**/*", output_erddap_xml=True)

    def test_compile_variables_xml_file(self):
        variables(
            TEST_FILE_PATH, "**/*", output_erddap_xml="test_compile_erddap_xml.xml"
        )

    # TODO add some tests for the cli method
