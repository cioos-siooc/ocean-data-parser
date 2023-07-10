import logging
import logging.config
import unittest
from pathlib import Path
from time import sleep

import pandas as pd
from click.testing import CliRunner
from utils import compare_text_files

from ocean_data_parser.batch import convert
from ocean_data_parser.batch.convert import (
    FileConversionRegistry,
    cli_files,
    load_config,
    main,
)

PACKAGE_PATH = Path(__file__).parent
TEST_REGISTRY_PATH = Path("tests/test_file_registry.csv")
TEST_FILE = Path("temp/test_file.csv")
TEST_REGISTRY = FileConversionRegistry(path=TEST_REGISTRY_PATH)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class ConfigLoadTests(unittest.TestCase):
    def test_default_config_load(self):
        config = load_config()
        assert isinstance(
            config, dict
        ), "Default loaded configuration is not a dictionary"

    def test_default_config_logging(self):
        config = load_config()
        logging.config.dictConfig(config["logging"])


class BatchModeTests(unittest.TestCase):
    def test_batch_conversion_onset_parser(self):
        config = load_config()
        config["input_path"] = "tests/parsers_test_files/onset/**/*.csv"
        config["parser"] = "onset.csv"
        config["overwrite"] = True
        main(config=config)

    def test_batch_cli_conversion_onset_parser(self):
        runner = CliRunner()
        result = runner.invoke(
            cli_files,
            ["--config=tests/batch_test_configs/batch_convert_test_onset_csv.yaml"],
        )
        assert result.exit_code == 0, result
        assert "Run batch conversion" in result.output

    def test_batch_cli_conversion_onset_parser_added_input(self):
        runner = CliRunner()
        result = runner.invoke(
            cli_files,
            [
                "--config=tests/batch_test_configs/batch_convert_test_onset_csv.yaml",
                "--add",
                "overwrite=True",
            ],
        )
        assert result.exit_code == 0, result
        assert "Run batch conversion" in result.output

    def test_batch_cli_new_config_creation(self):
        runner = CliRunner()
        new_config_test_file = Path("temp/test_config_copy.yaml")
        assert not new_config_test_file.exists()
        result = runner.invoke(
            cli_files, ["--new_config", new_config_test_file.as_posix()]
        )
        assert result.exit_code == 0, result
        assert new_config_test_file.exists()
        new_config_test_file.unlink()
        assert not new_config_test_file.exists()


def get_test_file_registry():
    test_file_registry = FileConversionRegistry(
        path=Path("tests/test_file_registry.csv")
    ).load()
    test_file = Path(test_file_registry.data.index[0])
    test_file.touch()
    return test_file_registry, test_file

