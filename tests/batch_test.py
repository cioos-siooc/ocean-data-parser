import logging
import logging.config
import unittest
from pathlib import Path

from ocean_data_parser.batch.main import FileConversionRegistry, conversion, load_config

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
    def test_batch_onset_parser(self):
        config = load_config()
        config["input"] = [
            {"path": "tests/parsers_test_files/onset/**/*.csv", "parser": "onset.csv"}
        ]
        conversion(config=config)


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
