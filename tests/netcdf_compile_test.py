import logging
from pathlib import Path
import unittest

from click.testing import CliRunner

from ocean_data_parser.compile.netcdf import variables, cli_variables

PACKAGE_PATH = Path(__file__).parent
logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")
TEST_FILE_PATH = PACKAGE_PATH / "parser_test_files" / "dfo" / "bio"


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

    # def test_compile_variables_cli(self):
    #     runner = CliRunner()
    #     result = runner.invoke(
    #         cli_variables,
    #         ["-i", "tests/dfo/bio", "-f", "'**/*_test.nc'"],
    #     )
    #     assert result.exit_code == 0, result.stderr
    #     assert "Compile NetCDF variables:" in result.output
    #     assert "standard_name" in result.output
    #     assert "</dataVariable>" not in result.output

    # def test_compile_variables_cli_xml_to_console(self):
    #     runner = CliRunner()
    #     result = runner.invoke(
    #         cli_variables,
    #         ["-i", "tests/dfo/bio", "-f", "'**/*_test.nc'", "-xml", "true"],
    #     )
    #     assert result.exit_code == 0, result
    #     assert "Compile NetCDF variables:" in result.output
    #     assert "</dataVariable>" in result.output

        # captured = capsys.readouterr()
        # assert captured

    # def test_batch_cli_conversion_onset_parser(self):
    #     runner = CliRunner()
    #     result = runner.invoke(
    #         cli_files,
    #         ["--config=tests/batch_test_configs/batch_convert_test_onset_csv.yaml"],
    #     )
    #     assert result.exit_code == 0, result
    #     assert "Run batch conversion" in result.output

    # def test_batch_cli_conversion_onset_parser_added_input(self):
    #     runner = CliRunner()
    #     result = runner.invoke(
    #         cli_files,
    #         [
    #             "--config=tests/batch_test_configs/batch_convert_test_onset_csv.yaml",
    #             "--add",
    #             "overwrite=True",
    #         ],
    #     )
    #     assert result.exit_code == 0, result
    #     assert "Run batch conversion" in result.output
