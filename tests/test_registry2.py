from pathlib import Path
import pytest
import pandas as pd
from ocean_data_parser.batch.registry2 import Registry
import math

TEST_MODULE_PATH = Path(__file__).parent
TEST_REGISTRY = TEST_MODULE_PATH / "test_file_registry.csv"


def get_test_registry():
    return Registry(
        source="test/*", destination="test_destination", registry_path=TEST_REGISTRY
    )


def make_test_file(dir, name, content):
    """Make a test file"""
    test_file = dir / name
    test_file.write_text(content)
    return test_file


class TestRegistryActions:
    def test_registry_init(self):
        # Test case for initializing registry
        registry = Registry(source="test/*", destination="test_destination")
        assert isinstance(registry, Registry)

    def test_registry_init_with_registry_path(self):
        registry = Registry(
            source="test/*", destination="test_destination", registry_path=TEST_REGISTRY
        )
        assert isinstance(registry, Registry)

    def test_new_registry(self):
        # Test case for generating registry
        registry = Registry(
            source="test/*",
            destination="test_destination",
        )
        new_registry = registry.new()
        assert isinstance(new_registry, pd.DataFrame)

    def test_load(self):
        # Test case for loading registry
        registry = get_test_registry()
        registry.load()
        assert isinstance(registry.data, pd.DataFrame)
        assert not registry.data.empty, "Registry is empty"
        assert isinstance(
            registry.data.index[0], Path
        ), "Registry index is not Path objects"

    def test_save(self, tmp_path):
        # Test case for saving registry
        registry = get_test_registry()
        test_registry = tmp_path / "test_registry.csv"
        assert not test_registry.exists(), "Test registry already exists"
        registry.registry_path = test_registry
        registry.save()
        assert test_registry.exists(), "Test registry was not saved"

    def test_save_elsewhere(self, tmp_path):
        # Test case for saving registry to a different location
        registry = get_test_registry()
        test_registry = tmp_path / "test_registry.csv"
        assert not test_registry.exists(), "Test registry already exists"
        registry.save(test_registry)
        assert test_registry.exists(), "Test registry was not saved"


class TestLocalFileActions:
    def test_file_mtime(self, tmp_path):
        # Test case for getting local file modification time
        tmp_file = make_test_file(tmp_path, "test_file.txt", "test file")
        mtime = Registry._get_local_file_mtime(tmp_file)
        assert isinstance(mtime, float)

    def test_file_size(self, tmp_path):
        # Test case for getting local file size
        tmp_file = make_test_file(tmp_path, "test_file.txt", "test file")
        size = Registry._get_local_file_size(tmp_file)
        assert size
        assert isinstance(size, int)
        assert size > 0

    def test_file_hash_default(self, tmp_path):
        # Test case for getting local file hash with default hash type
        tmp_file = make_test_file(tmp_path, "test_file.txt", "test file")
        file_hash = Registry._get_local_file_hash(tmp_file)
        assert file_hash
        assert isinstance(file_hash, str)
        assert len(file_hash) > 0

    @pytest.mark.parametrize("hash_type", ["sha256", "md5"])
    def test_file_hash(self, tmp_path, hash_type):
        # Test case for getting local file hash
        tmp_file = make_test_file(tmp_path, "test_file.txt", "test file")
        file_hash = Registry._get_local_file_hash(tmp_file, hash_type=hash_type)
        assert file_hash
        assert isinstance(file_hash, str)
        assert len(file_hash) > 0

    def test_get_files_from_local_source(tmp_path):
        # Test case for getting files from local source
        registry = Registry(
            source=Path("."), include=["**/*.py"], destination="tmp_path"
        )
        files = registry._get_files_from_local_source(
            files=None, hash=True, mtime=True, size=True
        )
        assert isinstance(files, list)
        assert len(files) > 0

        files = registry.new(data=files)
        assert isinstance(files, pd.DataFrame)
        assert len(files) > 0
        assert isinstance(files.index[0], Path)
        assert isinstance(files["hash"][0], str)


class TestRemoteFileActions:
    def test_get_remote_file_via_rsync(self, tmp_path):
        # Test case for getting remote file via rsync
        source_dir = tmp_path / "in"
        source_dir.mkdir()
        destination = tmp_path / "out"
        destination.mkdir()
        registry = Registry(source_dir, destination)
        test_source_file = source_dir / "test_file.txt"
        test_source_file.write_text("Test file")
        assert (
            list(destination.glob("*")) == []
        ), "Destination directory contains some files"

        source_files = registry._get_files_from_remote_source_via_rsync()

        assert (
            len(list(destination.glob("*"))) == 0
        ), "Destination directory contains some files, the default --dry-run flag should not copy files"
        assert isinstance(source_files, pd.DataFrame), "Source files is not a DataFrame"
        assert (
            len(source_files) == 1
        ), "Source files DataFrame does not have the correct number of rows"
        assert source_files.index[0] == test_source_file.relative_to(
            source_dir
        ), "Source files DataFrame does not have the correct file path"
        # TODO there's some timezone issues with macos at least which outputs local time in rsync
        assert source_files["mtime"].iloc[0] == math.floor(
            test_source_file.stat().st_mtime
        ), "Source files DataFrame does not have the correct mtime"  # mtime should be a float
        assert (
            source_files["size"][0] == test_source_file.stat().st_size
        ), "Source files DataFrame does not have the correct size"  # size should be an int
