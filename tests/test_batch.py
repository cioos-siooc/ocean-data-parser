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
    convert,
    load_config,
)
from ocean_data_parser.batch.utils import generate_output_path

MODULE_PATH = Path(__file__).parent
TEST_REGISTRY_PATH = Path("tests/test_file_registry.csv")
TEST_FILE = Path("temp/test_file.csv")
TEST_REGISTRY = FileConversionRegistry(path=TEST_REGISTRY_PATH)


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


def _get_config(
    input_path: str = "tests/parsers_test_files/onset/**/*.csv",
    cwd: Path = None,
    **kwargs,
):
    """Generate a batch configuration file"""
    config = {
        **load_config(),
        **kwargs,
        "input_path": input_path,
    }
    if cwd:
        config["registry"]["path"] = str(cwd / "registry.csv")
        config["output"]["path"] = str(cwd / "output")
        config["sentry"]["dsn"] = None
    return config


def _save_config(cwd, config):
    config_path = cwd / "config.yaml"
    with open(config_path, "w", encoding="UTF-8") as file:
        yaml.dump(config, file)

    return config_path


def _run_batch_process(config):
    registry = BatchConversion(config=config).run()
    assert not registry.data.empty
    assert not registry.data["error_message"].any()


class TestBatchMode:
    @pytest.mark.parametrize("multiprocessing", (1, 2, None))
    def test_batch_conversion_multiprocessing(self, tmpdir, multiprocessing):
        config = _get_config(cwd=tmpdir, multiprocessing=multiprocessing)
        _run_batch_process(config)

    @pytest.mark.parametrize(
        "key",
        (
            "output_path",
            "output_file_name",
            "output_file_preffix",
            "output_file_suffix",
            "output_format",
        ),
    )
    def test_batch_conversion_output_kwargs(self, key):
        batch = BatchConversion(**{key: "test"})
        key = key.replace("output_", "")
        assert batch.config["output"][key] == "test"

    @pytest.mark.parametrize(
        "key",
        ("registry_path", "registry_hashtype", "registry_block_size"),
    )
    def test_batch_conversion_registry_kwargs(self, key):
        batch = BatchConversion(**{key: "test"})
        key = key.replace("registry_", "")
        assert batch.config["registry"][key] == "test"

    def test_batch_conversion_dictionary_input(self):
        config = _get_config()
        batch = BatchConversion(config)
        assert batch
        assert batch.config
        assert batch.registry

    def test_failed_batch_conversion(self, tmpdir):
        test_file_path = str(tmpdir / "failed_cli_test_file.cnv")
        config = _get_config(
            cwd=tmpdir,
            input_path=test_file_path,
            parser="seabird.cnv",
            overwrite=True,
            multiprocessing=1,
            errors="ignore",
        )

        with open(test_file_path, "w") as file_handle:
            file_handle.write("test file")
        test_file_path = Path(test_file_path)
        registry = BatchConversion(config=config).run()
        assert not registry.data.empty
        assert test_file_path in registry.data.index
        assert "No columns to parse from file" in str(
            registry.data["error_message"][test_file_path]
        )


class TestBatchCLI:
    @staticmethod
    def _run_cli_batch_process(*args, isolated_directory=None):
        """Run Click cli code"""
        runner = CliRunner()
        if not isolated_directory:
            return runner.invoke(convert, args)

        with runner.isolated_filesystem(isolated_directory):
            return runner.invoke(convert, args)

    def test_batch_cli_conversion_onset_parser(self, tmpdir):
        config = _get_config(cwd=tmpdir)
        config_path = _save_config(tmpdir, config)
        result = self._run_cli_batch_process(
            f"--config={config_path}",
        )
        assert result.exit_code == 0, result.output
        assert (
            "Run conversion" in result.output
            or "Run parallel batch conversion" in result.output
        )

    def test_batch_cli_conversion_onset_parser_with_extra_inputs(self, tmpdir):
        config = _get_config(cwd=tmpdir)
        config_path = _save_config(tmpdir, config)
        result = self._run_cli_batch_process(
            "-i",
            "./**/*.csv",
            f"--config={config_path}",
            "--multiprocessing",
            3,
        )
        assert result.exit_code == 0, result.output
        assert (
            "Run conversion" in result.output
            or "Run parallel batch conversion" in result.output
        )

    def test_batch_cli_new_config_creation_output(self, tmpdir: Path):
        new_config_test_file = tmpdir / "test_config_copy.yaml"
        result = self._run_cli_batch_process("--new-config", str(new_config_test_file))
        assert (
            result.exit_code == 0
        ), f"new config failed with exit_code={result.exit_code}, result={result}"
        assert new_config_test_file.exists()

    def test_batch_cli_new_config_failed_creation_already_existing_file(
        self, tmpdir: Path
    ):
        new_config_test_file = tmpdir / "test_config_copy.yaml"
        new_config_test_file.write_text("test", encoding="UTF-8")
        assert new_config_test_file.exists()
        result = self._run_cli_batch_process("--new-config", new_config_test_file)
        assert result.exit_code == 1, result.output

    def test_batch_failed_cli_conversion_with_no_matching_inputs(self):
        result = self._run_cli_batch_process("-i", "*.csv")
        assert result.exit_code == 1
        assert result.output.startswith("ERROR"), f"unexpected output{result.output=}"

    def test_batch_failed_cli_conversion_with_argument_inputs(self):
        result = self._run_cli_batch_process("*.csv")
        assert result.exit_code == 2
        assert (
            "Error: Got unexpected extra argument" in result.output
        ), f"Unexpected output {result.output=}"

    def test_failed_cli_batch_conversion_with_ignore_errors(self, tmpdir):
        test_file_path = tmpdir / "failed_cli_test_file.cnv"
        config = _get_config(
            cwd=tmpdir,
            input_path=str(test_file_path),
            parser="seabird.cnv",
            overwrite=True,
            multiprocessing=1,
            errors="ignore",
        )

        config_path = _save_config(tmpdir, config)
        assert config_path.exists()

        # Save temp bad data file
        with open(test_file_path, "w", encoding="utf-8") as file_handle:
            file_handle.write("test file")

        result = self._run_cli_batch_process("--config", str(config_path))
        assert result.exit_code == 0, result.output
        # load registry
        registry = FileConversionRegistry(path=config["registry"]["path"])
        assert not registry.data.empty
        assert test_file_path in registry.data.index
        assert "No columns to parse from file" in str(
            registry.data["error_message"][test_file_path]
        )

    def test_failed_cli_batch_conversion_with_raise_errors(self, tmpdir):
        test_file_path = str(tmpdir / "failed_cli_test_file.cnv")
        config = _get_config(
            cwd=tmpdir,
            input_path=test_file_path,
            parser="seabird.cnv",
            overwrite=True,
            multiprocessing=1,
            errors="raise",
        )
        config["registry"]["path"] = "registry.csv"

        config_path = _save_config(tmpdir, config)
        assert config_path.exists()

        # Save temp bad data file
        with open(test_file_path, "w", encoding="utf-8") as file_handle:
            file_handle.write("test file")

        result = self._run_cli_batch_process("--config", str(config_path))
        # load registry
        assert result.exit_code == 1


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
        assert str(name) == "source_file.csv.nc"

    @pytest.mark.parametrize(
        "input,expected_path",
        (
            ({"path": "output"}, "output/source_file.csv.nc"),
            ({"file_name": "test"}, "test.nc"),
            ({"file_preffix": "test_"}, "test_source_file.csv.nc"),
            ({"file_suffix": "_test"}, "source_file.csv_test.nc"),
        ),
    )
    def test_generate_filename_with_unique_input(self, input, expected_path):
        name = generate_output_path(
            self._get_test_dataset(),
            **input,
            output_format=".nc",
        )
        assert isinstance(name, Path)
        assert name == Path(expected_path)

    def test_generate_filename_with_file_name(self):
        name = generate_output_path(
            self._get_test_dataset(),
            file_name="{organization}_{instrument}_test",
            output_format=".nc",
        )
        assert isinstance(name, Path)
        assert str(name) == "organization_InstrumentName_test.nc"

    def test_generate_filename_with_time(self):
        name = generate_output_path(
            self._get_test_dataset(),
            file_name="{organization}_{instrument}_{time_min:%Y%m%d}-{time_max:%Y%m%d}",
            output_format=".nc",
        )
        assert isinstance(name, Path)
        assert str(name) == "organization_InstrumentName_20220101-20220302.nc"

    def test_generate_filename_with_variable_attribute(self):
        name = generate_output_path(
            self._get_test_dataset(),
            file_name="{organization}_{instrument}_{variable_time_timezone}",
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
        assert str(name) == "test_source_file.csv.nc"

    def test_generate_filename_with_suffix(self):
        name = generate_output_path(self._get_test_dataset(), file_suffix="_test")
        assert str(name) == "source_file.csv_test.nc"

    def test_generate_filename_with_prefix_and_suffix(self):
        name = generate_output_path(
            self._get_test_dataset(), file_preffix="test_", file_suffix="_test"
        )
        assert str(name) == "test_source_file.csv_test.nc"

    def test_generate_filename_with_defaults(self):
        name = generate_output_path(
            self._get_test_dataset(),
            file_name="test_{missing_global}",
            defaults={"missing_global": "this-is-the-default"},
        )
        assert str(name) == "test_this-is-the-default.nc"


class TestBatchConversion:
    @pytest.mark.parametrize(
        "input_path",
        (
            "tests/parsers_test_files/dfo/odf/bio/**/*.ODF",
            "tests/parsers_test_files/dfo/odf/bio/CTD/*.ODF",
        ),
    )
    def test_batch_input_path(self, input_path):
        batch = BatchConversion(input_path=input_path)
        source_files = batch.get_source_files()
        assert source_files
        assert len(source_files) == len(list(glob(input_path)))
        assert set(source_files) == set(Path(file) for file in glob(input_path))

    @pytest.mark.parametrize(
        "exclude",
        (
            "tests/parsers_test_files/dfo/odf/bio/**/*.nc",
            "tests/parsers_test_files/dfo/odf/bio/CTD/*.nc",
            "tests/**/*.nc",
        ),
    )
    def test_batch_exclude_path(self, exclude):
        batch = BatchConversion(
            input_path="tests/parsers_test_files/dfo/odf/bio/CTD/*.*", exclude=exclude
        )
        excluded_files = batch.get_excluded_files()
        assert excluded_files
        assert set(excluded_files) == {str(file) for file in glob(exclude)}

        source_files = batch.get_source_files()
        assert source_files
        assert all(file.suffix == ".ODF" for file in source_files)
        assert set(source_files) == {
            Path(file)
            for file in glob("tests/parsers_test_files/dfo/odf/bio/CTD/*.ODF")
        }
