import logging
import logging.config
import unittest
from pathlib import Path

import xarray as xr
import pandas as pd
from click.testing import CliRunner
import pytest

from ocean_data_parser.batch.convert import (
    FileConversionRegistry,
    cli_files,
    load_config,
    main,
)
from ocean_data_parser.batch.utils import generate_output_path

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
    def test_batch_conversion_onset_parser_single_runner(self):
        config = load_config()
        config["input_path"] = "tests/parsers_test_files/onset/**/*.csv"
        config["parser"] = "onset.csv"
        config["overwrite"] = True
        config["multiprocessing"] = None
        config["file_output"]["path"] = "temp/batch/single_files/"
        main(config=config)

    def test_batch_conversion_onset_parser_multiprocessing(self):
        config = load_config()
        config["input_path"] = "tests/parsers_test_files/onset/**/*.csv"
        config["parser"] = "onset.csv"
        config["overwrite"] = True
        config["multiprocessing"] = 3
        config["file_output"]["path"] = "temp/batch/multiprocessing_files/"
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


test_ds = xr.Dataset()
test_ds.attrs["organization"] = "organization"
test_ds.attrs["instrument"] = "InstrumentName"
test_ds.attrs["instrument_serial_number"] = "64651354"
test_ds["time"] = pd.to_datetime(
    pd.Series(["2022-01-01T00:00:00Z", "2022-03-02T00:00:00Z"])
)
test_ds["time"].attrs["timezone"] = "UTC"


class TestBatchGenerateName:
    def test_generate_default_name(self):
        name = generate_output_path(test_ds)
        assert isinstance(name, Path)

    def test_generate_output_from_source_attribute(self):
        source_ds = test_ds.copy()
        source_ds.attrs["source"] = "source_file.csv"
        name = generate_output_path(source_ds)
        assert isinstance(name, Path)
        assert str(name) == "source_file.nc"

    def test_generate_filename_with_path(self):
        name = generate_output_path(
            test_ds, source="{organization}_{instrument}_test", output_format=".nc"
        )
        assert isinstance(name, Path)
        assert str(name) == "organization_InstrumentName_test.nc"

    def test_generate_filename_with_time(self):
        name = generate_output_path(
            test_ds,
            source="{organization}_{instrument}_{time_min:%Y%m%d}-{time_max:%Y%m%d}",
            output_format=".nc",
        )
        assert isinstance(name, Path)
        assert str(name) == "organization_InstrumentName_20220101-20220302.nc"

    def test_generate_filename_with_variable_attribute(self):
        name = generate_output_path(
            test_ds,
            source="{organization}_{instrument}_{variable_time_timezone}",
            output_format=".nc",
        )
        assert isinstance(name, Path)
        assert str(name) == "organization_InstrumentName_UTC.nc"

    def test_generate_filename_with_missing_source(self):
        fail_ds = test_ds.copy()
        fail_ds.attrs["source"] = None
        with pytest.raises(Exception) as e_info:
            name = generate_output_path(fail_ds)