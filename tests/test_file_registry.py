import unittest
from pathlib import Path
from time import sleep

import pandas as pd
from utils import compare_text_files

from ocean_data_parser.batch.convert import FileConversionRegistry

PACKAGE_PATH = Path(__file__).parent
TEST_REGISTRY_PATH = Path("tests/test_file_registry.csv")
TEST_FILE = Path("temp/test_file.csv")
TEST_REGISTRY = FileConversionRegistry(path=TEST_REGISTRY_PATH)

# Generate temporary test directory
TEST_TEMP_FOLDER = Path("temp")
TEST_TEMP_FOLDER.mkdir(parents=True, exist_ok=True)


class FileRegistryTests(unittest.TestCase):
    def _get_test_registry(self, update=True):
        registry = FileConversionRegistry(path=TEST_REGISTRY_PATH)
        if update:
            registry.update()
        return registry

    def make_test_file(self, filename: Path, content="this is a test file", mode="w"):
        if not filename.parent.exists():
            filename.parent.mkdir()

        with open(filename, mode) as file_handle:
            file_handle.write(content)
        assert filename.exists(), "test file wasn't created"
        return filename

    def test_init(self):
        file_registry = FileConversionRegistry()
        assert isinstance(
            file_registry, FileConversionRegistry
        ), "Didn't return  FileConversionRegistry object"

    def test_empty_registry(slef):
        file_registry = FileConversionRegistry(path="temp/test_empty_registry.csv")
        file_registry.save()
        file_registry.load()
        file_registry.update()
        file_registry.save()

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

    def test_registry(self):
        test_registry = self._get_test_registry()
        for attr in ["data", "hashtype", "path", "save", "load", "update"]:
            assert hasattr(
                test_registry, attr
            ), f"TEST_REGISTRY is missing attribute={attr}"
        assert isinstance(test_registry.data, pd.DataFrame)
        assert not test_registry.data.empty

    def test_registry_copy(self):
        deep_copied_file_registry = self._get_test_registry()
        deep_copied_file_registry.data["hash"] = 0
        assert (
            deep_copied_file_registry != self._get_test_registry()
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
        file_registry = self._get_test_registry()
        # Replace registry parameters
        file_registry.data["last_update"] = 0
        file_registry.data["hash"] = 0
        file_registry.load()
        assert (
            file_registry.data["last_update"] == 0
        ).all(), "last_update was updated with load()"
        assert (file_registry.data["hash"] == 0).all(), "hash was updated with load()"
        file_registry.load(overwrite=True)
        assert (
            file_registry.data["last_update"] != 0
        ).all(), "last_update wasn't updated with load(overwrite=Trues)"
        assert (
            file_registry.data["hash"] != 0
        ).all(), "hash wasn't updated with load()"

    def test_update(self):
        file_registry = self._get_test_registry(update=False)

        # Replace registry parameters
        file_registry.data["last_update"] = 0
        file_registry.data["hash"] = 0
        assert (
            file_registry != self._get_test_registry()
        ), "local test registry wasn't modify"

        file_registry.update()
        assert (
            file_registry.data["last_update"] != 0
        ).all(), "last_update wasn't updated wiht update()"
        assert (
            file_registry.data["hash"] != 0
        ).all(), "hash wasn't updated wiht update()"

    def test_update_specific_source(self):
        file_registry = self._get_test_registry()
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
        file_registry = self._get_test_registry()
        assert (
            "test" not in file_registry.data
        ), "new field 'test' was alreadyin the registry"
        file_registry.update_fields(test=True)
        assert "test" in file_registry.data, "new field wasn't added to the registry"
        assert file_registry.data[
            "test"
        ].all(), "new field wasn't added to the registry"

    def test_update_all_sources_field(self):
        file_registry = self._get_test_registry()
        file_registry.data["test"] = False
        assert (
            not file_registry.data["test"].any()
        ), "Test field wasn't all set to False"
        file_registry.update_fields(test=True)
        assert file_registry.data[
            "test"
        ].all(), "new field wasn't added to the registry"

    def test_update_single_source_missing_field(self):
        file_registry = self._get_test_registry()
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
        file_registry = self._get_test_registry()
        file_registry.data["test"] = False
        assert (
            not file_registry.data["test"].any()
        ), "Test field wasn't all set to False"
        file_registry.update_fields(sources=[file_registry.data.index[0]], test=True)
        assert file_registry.data.iloc[0][
            "test"
        ], "new field wasn't added to the registry"
        assert not (
             file_registry.data.iloc[1:]["test"]
        ).all(), "other fields weren't replaced by None"

    def test_update_multiple_fields(self):
        file_registry = self._get_test_registry()
        file_registry.update_fields(test=True, second_test=False)
        assert file_registry.data["test"].all(), "test input is missing"
        assert not (
            file_registry.data["second_test"]
        ).all(), "second_test input is missing"

    def test_save(self):
        file_registry = self._get_test_registry(update=False)

        file_registry.path = Path(str(file_registry.path).replace(".csv", "_temp.csv"))
        file_registry.save()
        differences = compare_text_files(
            str(self._get_test_registry().path), str(file_registry.path)
        )
        assert (
            not differences
        ), f"Resaving the intial test registry didn't produce a similar file: {differences}"

        file_registry.data.loc[file_registry.data.index[-1], "last_update"] += 100
        file_registry.save()
        differences = compare_text_files(
            str(self._get_test_registry().path), str(file_registry.path)
        )
        assert (
            len(differences) == 5
        ), f"Resaving the test registry after changes didn't produce the expected different file: {differences}"

    def test_get_source_base(self):
        file_registry = self._get_test_registry()
        file_registry.since = None
        assert ~file_registry.data.empty
        assert (
            not file_registry._is_new_file().all()
        ), "Failed to return all not new files"
        assert (
            not file_registry._is_different_hash().all()
        ), "test registry hashes are different"
        assert (
            not file_registry._is_different_mtime().all()
        ), "test registry mtimes are different"

        assert file_registry.since is None
        assert not file_registry._is_modified_since().all()

        assert not (
            file_registry._is_new_file() | file_registry._is_different_hash()
        ).all() == False, "returned source to be parsed"
        assert file_registry.get_source_files_to_parse() == []

    def test_get_sources_with_modified_hash_unchanged(self):
        file_registry = self._get_test_registry()
        changed_files = file_registry._is_different_hash()
        assert not changed_files.any()
        assert file_registry.get_source_files_to_parse() == []

    def test_get_sources_with_modified_hash(self):
        file_registry = self._get_test_registry()
        TEST_SAVE_PATH = self.make_test_file(
            Path("temp/test_get_sources_with_modified_hash.csv")
        )
        file_registry.add([TEST_SAVE_PATH])
        self.make_test_file(TEST_SAVE_PATH, " this is more content", mode="a")
        modified_sources = file_registry._is_different_hash()
        assert modified_sources.any()
        assert modified_sources[TEST_SAVE_PATH]
        assert file_registry.get_source_files_to_parse() == [TEST_SAVE_PATH]

    def update_test_file(
        self,
        file_registry,
        test_file_path: Path = Path("temp/test_file.csv"),
        dt: int = 4,
    ):
        test_file = self.make_test_file(test_file_path)
        file_registry.add([test_file])
        file_registry.update()
        test_file.touch()
        sleep(dt)
        file_registry.update()
        return file_registry

    def test_get_sources_with_since_timestamp_no_changes(self):
        file_registry = self._get_test_registry()
        file_registry.since = pd.Timestamp.utcnow().timestamp()
        modified_sources = file_registry._is_modified_since()
        assert not modified_sources.any()
        assert file_registry.get_source_files_to_parse() == []

    def test_get_sources_with_since_timestamp(self):
        file_registry = self._get_test_registry()
        file_registry.since = pd.Timestamp.utcnow()
        test_file = Path("temp/timestamp_test.csv")
        file_registry = self.update_test_file(file_registry, test_file)
        modified_sources = file_registry._is_modified_since()
        assert modified_sources.any(), "mtime check failed to return something"
        assert modified_sources.loc[test_file]
        assert file_registry.get_source_files_to_parse() == [test_file]

    def test_get_sources_with_since_timestamp_str(self):
        file_registry = self._get_test_registry()
        file_registry.since = pd.Timestamp.utcnow().isoformat()
        test_file = Path("temp/timestamp_test.csv")
        file_registry = self.update_test_file(file_registry, test_file)
        modified_sources = file_registry._is_modified_since()
        assert modified_sources.any(), "mtime check failed to return something"
        assert modified_sources.loc[test_file]
        assert file_registry.get_source_files_to_parse() == [test_file]

    def test_get_sources_with_since_timedelta(self):
        file_registry = self._get_test_registry()
        file_registry.since = "10s"
        test_file = Path(
            "temp/test_get_sources_with_modified_mtime_time_difference.csv"
        )
        file_registry = self.update_test_file(file_registry, test_file)
        modified_sources = file_registry._is_modified_since()
        assert modified_sources.any(), "mtime check failed to return something"
        assert modified_sources.loc[test_file]
        assert file_registry.get_source_files_to_parse() == [test_file]

    def test_get_missing_files(self):
        file_registry = self._get_test_registry()
        TEST_SAVE_PATH = self.make_test_file(Path("test_get_missing_files.csv"))
        file_registry.add([TEST_SAVE_PATH])
        file_registry.update()
        assert file_registry.get_missing_sources() == []
        TEST_SAVE_PATH.unlink()
        missing_files = file_registry.get_missing_sources()
        assert missing_files, "failed to detect missing file"
        assert missing_files == [TEST_SAVE_PATH]
