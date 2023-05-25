import logging
import logging.config
import unittest
from pathlib import Path

from click.testing import CliRunner

from ocean_data_parser.batch.convert import (
    FileConversionRegistry,
    files,
    load_config,
    cli_files,
)

PACKAGE_PATH = Path(__file__).parent
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
        files(config=config)

    def test_batch_cli_conversion_onset_parser(self):
        runner = CliRunner()
        result = runner.invoke(
            cli_files,
            ["--config=tests/batch_test_configs/batch_convert_test_onset_csv.yaml"],
        )
        assert result.exit_code == 0
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
        assert result.exit_code == 0
        assert "Run batch conversion" in result.output


class FileRegistryTests(unittest.TestCase):
    def test_file_registry_init(self):
        file_registry = FileConversionRegistry()
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"

    def test_file_registry_init_csv(self):
        file_registry = FileConversionRegistry(path="test_registry.csv")
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"

    def test_file_registry_init_csv(self):
        file_registry = FileConversionRegistry(path="test_registry.parquet")
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"

    def test_file_registry_load_csv_not_available(self):
        file_registry = FileConversionRegistry(path="test_registry.csv").load()
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"

    def test_file_registry_load_parquet_not_available(self):
        file_registry = FileConversionRegistry(path="test_registry.parquet").load()
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"
