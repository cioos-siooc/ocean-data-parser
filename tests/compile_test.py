import logging
from pathlib import Path
import unittest

# from click.testing import CliRunner

from ocean_data_parser.compile.netcdf import variables

PACKAGE_PATH = Path(__file__).parent
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class CompileNetcdfVariablesTests:
    def test_compile_variables_default(self):
        test_files = PACKAGE_PATH / "parser_test_files" / "dfo" / "bio"
        variables(test_files, "**/*")
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
