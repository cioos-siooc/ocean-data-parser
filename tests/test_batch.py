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
    def test_batch_conversion_onset_parser_single_runner(self, tmp_path):
        self._run_batch_processing(
            1, tmp_path / "single", tmp_path / "single_registry.csv"
        )

    def test_batch_conversion_onset_parser_multiprocessing_2_workers(self, tmp_path):
        self._run_batch_processing(
            2, tmp_path / "2_workers", tmp_path / "2_workers_registry.csv"
        )

    def test_batch_conversion_onset_parser_multiprocessing_all_workers(self, tmp_path):
        self._run_batch_processing(
            True,
            tmp_path / "multiprocessing_files",
            tmp_path / "multi_registry.csv",
        )

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
        config = load_config()
        test_file_path = str(tmp_path / "failed_cli_test_file.cnv")
        registry_path = str(tmp_path / "failed_cli_registry.csv")
        config_path = tmp_path / "failed_cli_config.yaml"

        with open(test_file_path, "w") as file_handle:
            file_handle.write("test file")

        config["input_path"] = test_file_path
        config["parser"] = "seabird.cnv"
        config["errors"] = "ignore"
        config["overwrite"] = True
        config["multiprocessing"] = True
        config["file_output"]["path"] = str(tmp_path / "failed_files/")
        config["file_output"]["source"] = "{source}"
        config["registry"]["path"] = registry_path
        config["sentry"]["dsn"] = None

        # Save config to yaml
        with open(config_path, "w", encoding="utf-8") as file:
            yaml.dump(config, file)

        runner = CliRunner()
        result = runner.invoke(
            cli_files,
            [f"--config={config_path}"],
        )
        assert result.exit_code == 0, result
        # load registry
        registry = FileConversionRegistry(path=registry_path)
        assert not registry.data.empty
        assert test_file_path in registry.data.index
        assert "No columns to parse from file" in str(
            registry.data["error_message"][test_file_path]
        )

        # Delete test files
        Path(test_file_path).unlink()
        Path(registry_path).unlink()

    def test_failed_batch_conversion(self, tmp_path):
        config = load_config()
        test_file_path = str(tmp_path / "bad_test_file.cnv")
        registry_path = str(tmp_path / "failed_registry.csv")

        with open(test_file_path, "w") as file_handle:
            file_handle.write("test file")

        config["input_path"] = test_file_path
        config["parser"] = "seabird.cnv"
        config["errors"] = "ignore"
        config["overwrite"] = True
        config["multiprocessing"] = True
        config["file_output"]["path"] = str(tmp_path / "failed_files/")
        config["file_output"]["source"] = "{source}"
        config["registry"]["path"] = registry_path
        config["sentry"]["dsn"] = None
        registry = BatchConversion(config=config).run()
        assert not registry.data.empty
        assert test_file_path in registry.data.index
        assert "No columns to parse from file" in str(
            registry.data["error_message"][test_file_path]
        )

    def test_batch_cli_conversion_onset_parser(self):
        runner = CliRunner()
        result = runner.invoke(
            cli_files,
            ["--config=tests/batch_test_configs/batch_convert_test_onset_csv.yaml"],
            env={"LOGURU_LEVEL": "INFO"},
        )
        assert result.exit_code == 0, result
        assert (
            "Run conversion" in result.output
            or "Run parallel batch conversion" in result.output
        )

    def test_batch_cli_new_config_creation(self):
        runner = CliRunner()
        new_config_test_file = Path("temp/test_config_copy.yaml")
        if new_config_test_file.exists():
            new_config_test_file.unlink()

        assert not new_config_test_file.exists()
        result = runner.invoke(cli_files, ["--new_config", str(new_config_test_file)])
        assert (
            result.exit_code == 0
        ), f"new config failed with exit_code={result.exit_code}, result={result}"
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
test_ds.attrs["source"] = "source_file.csv"
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
        with pytest.raises(Exception):
            generate_output_path(fail_ds)

    def test_generate_filename_with_prefix(self):
        name = generate_output_path(test_ds, file_preffix="test_")
        assert str(name) == "test_source_file.nc"

    def test_generate_filename_with_suffix(self):
        name = generate_output_path(test_ds, file_suffix="_test")
        assert str(name) == "source_file_test.nc"

    def test_generate_filename_with_prefix_and_suffix(self):
        name = generate_output_path(test_ds, file_preffix="test_", file_suffix="_test")
        assert str(name) == "test_source_file_test.nc"

    def test_generate_filename_with_defaults(self):
        name = generate_output_path(
            test_ds,
            source="test_{missing_global}",
            defaults={"missing_global": "this-is-the-default"},
        )
        assert str(name) == "test_this-is-the-default.nc"
