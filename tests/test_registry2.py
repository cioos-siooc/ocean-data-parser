from pathlib import Path
import pytest
import pandas as pd
from ocean_data_parser.batch.registry2 import Registry
import math

TEST_MODULE_PATH = Path(__file__).parent
TEST_REGISTRY = TEST_MODULE_PATH / "test_file_registry.csv"


def make_test_file(dir, name, content):
    """Make a test file"""
    test_file = dir / name
    test_file.write_text(content)
    return test_file


def test_registry_init():
    # Test case for initializing registry
    registry = Registry(source="test/*", destination="test_destination")
    assert isinstance(registry, Registry)


def test_generate_registry():
    # Test case for generating registry
    registry = Registry()
    registry.generate_registry()
    assert isinstance(registry.data, pd.DataFrame)


def test_load():
    # Test case for loading registry
    registry = Registry(
        source="test/*", destination="test_output/", registry_path=TEST_REGISTRY
    )
    registry.load()
    assert isinstance(registry.data, pd.DataFrame)
    assert not registry.data.empty, "Registry is empty"
    assert isinstance(
        registry.data.index[0], Path
    ), "Registry index is not Path objects"


def test_save(tmp_path):
    # Test case for saving registry
    registry = Registry()
    test_registry = tmp_path / "test_registry.csv"
    assert not test_registry.exists(), "Test registry already exists"
    registry.registry_path = test_registry
    registry.save()
    assert test_registry.exists(), "Test registry was not saved"


def test_file_mtime(tmp_path):
    # Test case for getting local file modification time
    tmp_file = make_test_file(tmp_path, "test_file.txt", "test file")
    mtime = Registry._get_local_file_mtime(tmp_file)
    assert isinstance(mtime, float)


def test_file_size(tmp_path):
    # Test case for getting local file size
    tmp_file = make_test_file(tmp_path, "test_file.txt", "test file")
    size = Registry._get_local_file_size(tmp_file)
    assert size
    assert isinstance(size, int)
    assert size > 0


def test_file_hash_default(tmp_path):
    # Test case for getting local file hash with default hash type
    tmp_file = make_test_file(tmp_path, "test_file.txt", "test file")
    file_hash = Registry._get_local_file_hash(tmp_file)
    assert file_hash
    assert isinstance(file_hash, str)
    assert len(file_hash) > 0


@pytest.mark.parametrize("hash_type", ["sha256", "md5"])
def test_file_hash(tmp_path, hash_type):
    # Test case for getting local file hash
    tmp_file = make_test_file(tmp_path, "test_file.txt", "test file")
    file_hash = Registry._get_local_file_hash(tmp_file, hash_type=hash_type)
    assert file_hash
    assert isinstance(file_hash, str)
    assert len(file_hash) > 0


def test_get_remote_file_via_rsync(tmp_path):
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

    source_files = registry._get_remote_file_via_rsync()

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
    assert source_files["hash"][0] == registry._get_local_file_hash(
        test_source_file, registry.hash_type, registry.blocksize
    ), "Source files DataFrame does not have the correct hash"  # hash should be a string


# def test_get_source_files():
#     # Test case for getting source files
#     registry = Registry()
#     source_files = ["/path/to/file1", "/path/to/file2"]
#     data = registry.get_source_files(source_files)
#     assert isinstance(data, pd.DataFrame)


# def test_compare():
#     # Test case for comparing data
#     registry = Registry()
#     data = pd.DataFrame()
#     registry.compare(data)
#     # Add assertions here


# def test_download_remote_source_files():
#     # Test case for downloading remote source files
#     registry = Registry()
#     new_files = ["/path/to/file1", "/path/to/file2"]
#     registry.download_remote_source_files(new_files)
#     # Add assertions here


# def test_update():
#     # Test case for updating registry
#     registry = Registry()
#     new_files = pd.DataFrame()
#     modified_files = pd.DataFrame()
#     deleted_files = pd.DataFrame()
#     registry.update(new_files, modified_files, deleted_files)
#     # Add assertions here


# def test_sync_registry():
#     # Test case for syncing registry
#     registry = Registry()
#     new_files = pd.DataFrame()
#     modified_files = pd.DataFrame()
#     deleted_files = pd.DataFrame()
#     result = registry.sync_registry()
#     assert isinstance(result, list)
#     # Add assertions here
