import filecmp
import logging
import logging.config
import unittest
from pathlib import Path
from time import sleep

from click.testing import CliRunner

from ocean_data_parser.batch.convert import (
    FileConversionRegistry,
    cli_files,
    files,
    load_config,
)
from utils import compare_text_files

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


def get_test_file_registry():
    test_file_registry = FileConversionRegistry(
        path=Path("tests/test_file_registry.csv")
    ).load()
    test_file = Path(test_file_registry.data.index[0])
    test_file.touch()
    return test_file_registry, test_file


TEST_REGISTRY_PATH = Path("tests/test_file_registry.csv")
TEST_REGISTRY = FileConversionRegistry(path=TEST_REGISTRY_PATH)


class FileRegistryTests(unittest.TestCase):
    def make_test_file(self, filename, content="this is a test file", mode="w"):
        test_temp_folder = Path("temp")
        if test_temp_folder not in filename.parents:
            filename = test_temp_folder / filename
        if not test_temp_folder.exists():
            test_temp_folder.mkdir()

        with open(filename, mode) as file_handle:
            file_handle.write(content)
        assert filename.exists(), "test file wasn't created"
        return filename

    def test_init(self):
        file_registry = FileConversionRegistry()
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"

    def test_init_missing_csv(self):
        file_registry = FileConversionRegistry(path="test_registry.csv")
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"

    def test_init_missing_parquet(self):
        file_registry = FileConversionRegistry(path="test_registry.parquet")
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"

    def test_load_csv_available_with_str(self):
        file_registry = FileConversionRegistry(path="tests/test_file_registry.csv")
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"
        assert not file_registry.data.empty, "registry is an empty dataframe"

    def test_load_csv_available_with_str_as_arg(self):
        file_registry = FileConversionRegistry("tests/test_file_registry.csv")
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"
        assert not file_registry.data.empty, "registry is an empty dataframe"

    def test_load_csv_available_with_Path(self):
        file_registry = FileConversionRegistry(
            path=Path("tests/test_file_registry.csv")
        )
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"
        assert not file_registry.data.empty, "registry is an empty dataframe"

    def test_registry_copy(self):
        deep_copied_file_registry = TEST_REGISTRY.deepcopy()
        deep_copied_file_registry.data["hash"] = 0
        assert (
            deep_copied_file_registry != TEST_REGISTRY
        ), "Deed copied registry after modification changed the original"

        copied_file_registry = deep_copied_file_registry.copy()
        copied_file_registry.data["hash"] = 2
        assert (
            deep_copied_file_registry != copied_file_registry
        ), "Copied registry after modification changed the original"

        not_copied_registry = deep_copied_file_registry
        not_copied_registry.data["hash"] = 2
        assert (
            deep_copied_file_registry == not_copied_registry
        ), "Registry after modification didn't changed the original"

    def test_load(self):
        file_registry = TEST_REGISTRY.deepcopy()
        # Replace registry parameters
        file_registry.data["last_update"] = 0
        file_registry.data["hash"] = 0
        file_registry.load()
        assert (
            file_registry.data["last_update"] != 0
        ).all(), "last_update wasn't updated wiht load()"
        assert (
            file_registry.data["hash"] != 0
        ).all(), "hash wasn't updated wiht load()"

    def test_update(self):
        file_registry = TEST_REGISTRY.deepcopy()

        # Replace registry parameters
        file_registry.data["last_update"] = 0
        file_registry.data["hash"] = 0
        assert file_registry != TEST_REGISTRY, "local test registry wasn't modify"

        file_registry.update()
        assert (
            file_registry.data["last_update"] != 0
        ).all(), "last_update wasn't updated wiht update()"
        assert (
            file_registry.data["hash"] != 0
        ).all(), "hash wasn't updated wiht update()"

    def test_update_specific_source(self):
        file_registry = TEST_REGISTRY.deepcopy()
        # Replace registry parameters
        file_registry.data["last_update"] = 0
        file_registry.data["hash"] = 0
        file_registry.update([file_registry.data.index[0]])
        assert (
            file_registry.data.iloc[0]["last_update"] != 0
        ), "last_update wasn't updated wiht update(source)"
        assert (
            file_registry.data.iloc[1:]["last_update"] == 0
        ).all(), "last_update source!=source shouldn't be updated with update(source)"
        assert (
            file_registry.data.iloc[0]["hash"] != 0
        ), "hash wasn't updated wiht update(source)"
        assert (
            file_registry.data.iloc[1:]["hash"] == 0
        ).all(), "hash source!=source shouldn't be updated with update(source)"

    def test_update_all_sources_missing_field(self):
        file_registry = TEST_REGISTRY.deepcopy()
        assert (
            "test" not in file_registry.data
        ), "new field 'test' was alreadyin the registry"
        file_registry.update_fields(test=True)
        assert "test" in file_registry.data, "new field wasn't added to the registry"
        assert file_registry.data[
            "test"
        ].all(), "new field wasn't added to the registry"

    def test_update_all_sources_field(self):
        file_registry = TEST_REGISTRY.deepcopy()
        file_registry.data["test"] = False
        assert (
            file_registry.data["test"].any() == False
        ), "Test field wasn't all set to False"
        file_registry.update_fields(test=True)
        assert file_registry.data[
            "test"
        ].all(), "new field wasn't added to the registry"

    def test_update_single_source_missing_field(self):
        file_registry = TEST_REGISTRY.deepcopy()
        assert (
            "test" not in file_registry.data
        ), "new field 'test' was alreadyin the registry"
        file_registry.update_fields(sources=file_registry.data.index[0], test=True)
        assert "test" in file_registry.data, "new field wasn't added to the registry"
        assert file_registry.data.iloc[0][
            "test"
        ], "new field wasn't added to the registry"
        assert (
            file_registry.data.iloc[1:]["test"].isna().all()
        ), "other fields weren't replaced by None"

    def test_update_single_source_missing_field(self):
        file_registry = TEST_REGISTRY.deepcopy()
        file_registry.data["test"] = False
        assert (
            file_registry.data["test"].any() == False
        ), "Test field wasn't all set to False"
        file_registry.update_fields(sources=file_registry.data.index[0], test=True)
        assert file_registry.data.iloc[0][
            "test"
        ], "new field wasn't added to the registry"
        assert (
            file_registry.data.iloc[1:]["test"] == False
        ).all(), "other fields weren't replaced by None"

    def test_update_multiple_fields(self):
        file_registry = TEST_REGISTRY.deepcopy()
        file_registry.update_fields(test=True, second_test=False)
        assert file_registry.data["test"].all(), "test input is missing"
        assert (
            file_registry.data["second_test"] == False
        ).all(), "second_test input is missing"

    def test_save(self):
        file_registry = TEST_REGISTRY.deepcopy()

        file_registry.path = Path(str(file_registry.path).replace(".csv", "_temp.csv"))
        file_registry.save()
        differences = compare_text_files(
            str(TEST_REGISTRY.path), str(file_registry.path)
        )
        assert (
            not differences
        ), f"Resaving the intial test registry didn't produce a similar file: {differences}"

        file_registry.data["last_update"].iloc[-1] += 100
        file_registry.save()
        differences = compare_text_files(
            str(TEST_REGISTRY.path), str(file_registry.path)
        )
        assert (
            len(differences) == 5
        ), f"Resaving the test registry after changes didn't produce the expected different file: {differences}"

    def test_get_sources_with_modified_hash_unchanged(self):
        file_registry = TEST_REGISTRY.deepcopy()
        file_registry.update()
        changed_files = file_registry.get_sources_with_modified_hash()
        assert changed_files == []

    def test_get_sources_with_modified_hash_single_source(self):
        file_registry = TEST_REGISTRY.deepcopy()
        file_registry.update()
        changed_files = file_registry.get_sources_with_modified_hash(
            [file_registry.data.index[0]]
        )
        assert changed_files == []

    def test_get_sources_with_modified_hash(self):
        file_registry = TEST_REGISTRY.deepcopy()
        file_registry.update()
        TEST_SAVE_PATH = self.make_test_file(
            Path("test_get_sources_with_modified_hash.csv")
        )
        file_registry.add_missing([TEST_SAVE_PATH])
        self.make_test_file(TEST_SAVE_PATH, " this is more content", mode="a")
        modified_sources = file_registry.get_sources_with_modified_hash()
        assert modified_sources
        assert modified_sources == [TEST_SAVE_PATH]

    def test_get_sources_with_modified_mtime_unchanged(self):
        file_registry = TEST_REGISTRY.deepcopy()
        file_registry.update()
        changed_files = file_registry.get_sources_with_mtime()
        assert changed_files == []

    def test_get_sources_with_modified_mtime_single_source(self):
        file_registry = TEST_REGISTRY.deepcopy()
        file_registry.update()
        changed_files = file_registry.get_sources_with_mtime(
            [file_registry.data.index[0]]
        )
        assert changed_files == []

    def test_get_sources_with_modified_mtime(self):
        file_registry = TEST_REGISTRY.deepcopy()
        file_registry.update()
        TEST_SAVE_PATH = self.make_test_file(
            Path("test_get_sources_with_modified_mtime.csv")
        )
        file_registry.add_missing([TEST_SAVE_PATH])
        self.make_test_file(TEST_SAVE_PATH, " this is more content", mode="a")
        modified_sources = file_registry.get_sources_with_mtime()
        assert modified_sources
        assert modified_sources == [TEST_SAVE_PATH]

    def test_get_sources_with_modified_mtime_time_difference(self):
        file_registry = TEST_REGISTRY.deepcopy()
        TEST_SAVE_PATH = self.make_test_file(
            Path("test_get_sources_with_modified_mtime_time_difference.csv")
        )
        file_registry.add_missing([TEST_SAVE_PATH])
        file_registry.update()
        sleep(4)
        TEST_SAVE_PATH.touch()
        modified_sources_1s = file_registry.get_sources_with_mtime(time_difference=1)
        modified_sources_10ks = file_registry.get_sources_with_mtime(
            time_difference=10000
        )
        assert modified_sources_1s, "mtime check failed to return something"
        assert modified_sources_1s == [TEST_SAVE_PATH]
        assert modified_sources_10ks == []

    def test_get_missing_files(self):
        file_registry = TEST_REGISTRY.deepcopy()
        TEST_SAVE_PATH = self.make_test_file(Path("test_get_missing_files.csv"))
        file_registry.add_missing([TEST_SAVE_PATH])
        file_registry.update()
        assert file_registry.get_missing_sources() == []
        TEST_SAVE_PATH.unlink()
        missing_files = file_registry.get_missing_sources()
        assert missing_files, "failed to detect missing file"
        assert missing_files == [TEST_SAVE_PATH]