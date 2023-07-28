import logging
import logging.config
from pathlib import Path

import pandas as pd
import pytest
import xarray as xr
import yaml
from click.testing import CliRunner

from ocean_data_parser.batch.config import glob
from ocean_data_parser.batch.convert import (
    BatchConversion,
    FileConversionRegistry,
    cli_files,
    load_config,
)
from ocean_data_parser.batch.utils import generate_output_path

MODULE_PATH = Path(__file__).parent
TEST_REGISTRY_PATH = Path("tests/test_file_registry.csv")
TEST_FILE = Path("temp/test_file.csv")
TEST_REGISTRY = FileConversionRegistry(path=TEST_REGISTRY_PATH)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestConfigLoad:
    def test_default_config_load(self):
        config = load_config()
        assert isinstance(
            config, dict
        ), "Default loaded configuration is not a dictionary"

    def test_default_config_logging(self):
        load_config()

    def test_config_glob(self):
        paths = glob("ocean_data_parser/**/*.py")
        assert paths
        path_list = list(paths)
        assert all(isinstance(path, Path) for path in path_list)
        assert path_list
        assert len(path_list) > 10


class TestBatchMode:
    @staticmethod
    def _get_config(input_path=None, cwd=None, **kwargs):
        """Generate a batch configuration file"""
        config = {
            **load_config(),
            **kwargs,
            "input_path": input_path or "tests/parsers_test_files/onset/**/*.csv",
        }
        if cwd:
            config["registry"]["path"] = str(cwd / "registry.csv")
            config["file_output"]["path"] = str(cwd / "output")
            config["sentry"]["dsn"] = None
        return config

    @staticmethod
    def _save_config(cwd, config):
        config_path = cwd / "config.yaml"
        with open(config_path, "w", encoding="UTF-8") as file:
            yaml.dump(config, file)

        return config_path

    @staticmethod
    def _run_batch_process(config):
        registry = BatchConversion(config=config).run()
        assert not registry.data.empty
        assert not registry.data["error_message"].any()

    @staticmethod
    def _run_cli_batch_process(config_path):
        runner = CliRunner()
        result = runner.invoke(
            cli_files,
            [f"--config={config_path}"],
        )
        assert result.exit_code == 0, result

    @pytest.mark.parametrize("multiprocessing", [1, 2, None])
    def test_batch_conversion_multiprocessing(self, tmp_path, multiprocessing):
        config = self._get_config(cwd=tmp_path, multiprocessing=multiprocessing)
        self._run_batch_process(config)

    def _run_batch_processing(self, multiprocessing, output_path, registry_path):
        config = {
            **load_config(),
            "input_path": "tests/parsers_test_files/onset/**/*.csv",
            "parser": "onset.csv",
            "overwrite": True,
            "multiprocessing": multiprocessing,
            "file_output": {
                "path": output_path,
                "source": "{instrument_sn}",
            },
            "registry": {"path": registry_path},
        }
        registry = BatchConversion(config=config).run()
        assert not registry.data.empty
        assert not registry.data["error_message"].any()

    def test_failed_cli_batch_conversion(self, tmp_path):
        test_file_path = str(tmp_path / "failed_cli_test_file.cnv")
        config = self._get_config(
            cwd=tmp_path,
            input_path=test_file_path,
            parser="seabird.cnv",
            overwrite=True,
            multiprocessing=1,
            errors="ignore",
        )

        config_path = self._save_config(tmp_path, config)

        # Save temp bad data file
        with open(test_file_path, "w", encoding="utf-8") as file_handle:
            file_handle.write("test file")

        self._run_cli_batch_process(config_path)
        # load registry
        registry = FileConversionRegistry(path=config["registry"]["path"])
        assert not registry.data.empty
        assert test_file_path in registry.data.index
        assert "No columns to parse from file" in str(
            registry.data["error_message"][test_file_path]
        )

    def test_failed_batch_conversion(self, tmp_path):
        test_file_path = str(tmp_path / "failed_cli_test_file.cnv")
        config = self._get_config(
            cwd=tmp_path,
            input_path=test_file_path,
            parser="seabird.cnv",
            overwrite=True,
            multiprocessing=1,
            errors="ignore",
        )

        with open(test_file_path, "w") as file_handle:
            file_handle.write("test file")

        registry = BatchConversion(config=config).run()
        assert not registry.data.empty
        assert test_file_path in registry.data.index
        assert "No columns to parse from file" in str(
            registry.data["error_message"][test_file_path]
        )

    def test_batch_cli_conversion_onset_parser(self, tmp_path):
        config = self._get_config(cwd=tmp_path)
        config_path = self._save_config(tmp_path, config)
        runner = CliRunner()
        result = runner.invoke(
            cli_files,
            [f"--config={config_path}"],
        )
        assert result.exit_code == 0, result.output
        assert (
            "Run conversion" in result.output
            or "Run parallel batch conversion" in result.output
        )

    def test_batch_cli_new_config_creation(self, tmp_path):
        runner = CliRunner()
        new_config_test_file = tmp_path / "test_config_copy.yaml"
        result = runner.invoke(cli_files, ["--new_config", str(new_config_test_file)])
        assert (
            result.exit_code == 0
        ), f"new config failed with exit_code={result.exit_code}, result={result}"
        assert new_config_test_file.exists()
        new_config_test_file.unlink()
        assert not new_config_test_file.exists()


test_ds = xr.Dataset()
test_ds.attrs["organization"] = "organization"
test_ds.attrs["instrument"] = "InstrumentName"
test_ds.attrs["instrument_serial_number"] = "64651354"
test_ds.attrs["source"] = "source_file.csv"
test_ds["time"] = pd.to_datetime(
    pd.Series(["2022-01-01T00:00:00Z", "2022-03-02T00:00:00Z"])
)
test_ds["time"].attrs["timezone"] = "UTC"


class TestBatchGenerateName:
    @staticmethod
    def _get_test_dataset():
        ds = xr.Dataset()
        ds.attrs["organization"] = "organization"
        ds.attrs["instrument"] = "InstrumentName"
        ds.attrs["instrument_serial_number"] = "64651354"
        ds.attrs["source"] = "source_file.csv"
        ds["time"] = pd.to_datetime(
            pd.Series(["2022-01-01T00:00:00Z", "2022-03-02T00:00:00Z"])
        )
        ds["time"].attrs["timezone"] = "UTC"
        return ds

    def test_generate_default_name(self):
        name = generate_output_path(self._get_test_dataset())
        assert isinstance(name, Path)

    def test_generate_output_from_source_attribute(self):
        source_ds = self._get_test_dataset()
        source_ds.attrs["source"] = "source_file.csv"
        name = generate_output_path(source_ds)
        assert isinstance(name, Path)
        assert str(name) == "source_file.nc"

    def test_generate_filename_with_path(self):
        name = generate_output_path(
            self._get_test_dataset(),
            source="{organization}_{instrument}_test",
            output_format=".nc",
        )
        assert isinstance(name, Path)
        assert str(name) == "organization_InstrumentName_test.nc"

    def test_generate_filename_with_time(self):
        name = generate_output_path(
            self._get_test_dataset(),
            source="{organization}_{instrument}_{time_min:%Y%m%d}-{time_max:%Y%m%d}",
            output_format=".nc",
        )
        assert isinstance(name, Path)
        assert str(name) == "organization_InstrumentName_20220101-20220302.nc"

    def test_generate_filename_with_variable_attribute(self):
        name = generate_output_path(
            self._get_test_dataset(),
            source="{organization}_{instrument}_{variable_time_timezone}",
            output_format=".nc",
        )
        assert isinstance(name, Path)
        assert str(name) == "organization_InstrumentName_UTC.nc"

    def test_generate_filename_with_missing_source(self):
        fail_ds = self._get_test_dataset()
        fail_ds.attrs["source"] = None
        with pytest.raises(Exception):
            generate_output_path(fail_ds)

    def test_generate_filename_with_prefix(self):
        name = generate_output_path(self._get_test_dataset(), file_preffix="test_")
        assert str(name) == "test_source_file.nc"

    def test_generate_filename_with_suffix(self):
        name = generate_output_path(self._get_test_dataset(), file_suffix="_test")
        assert str(name) == "source_file_test.nc"

    def test_generate_filename_with_prefix_and_suffix(self):
        name = generate_output_path(
            self._get_test_dataset(), file_preffix="test_", file_suffix="_test"
        )
        assert str(name) == "test_source_file_test.nc"

    def test_generate_filename_with_defaults(self):
        name = generate_output_path(
            self._get_test_dataset(),
            source="test_{missing_global}",
            defaults={"missing_global": "this-is-the-default"},
        )
        assert str(name) == "test_this-is-the-default.nc"
