import logging
import re
from pathlib import Path

import pytest
from xarray import Dataset

from ocean_data_parser import read
from ocean_data_parser.parsers import onset

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

auto_detection_ignore = [
    "CTD_1994038_147_1_DN.ODF",
    "CTD_2020003_004_1_DN.ODF",
    "cab041_2023_metqa_updated.csv",
]
auto_detection_ignore_extensions = (".nc", ".DS_Store")


@pytest.mark.parametrize(
    "file",
    [
        str(file)
        for file in Path("tests/parsers_test_files").glob("**/*.*")
        if not file.name.endswith(auto_detection_ignore_extensions)
        and file.name not in auto_detection_ignore
    ],
)
def test_automated_parser_detection(file):
    parser = read.detect_file_format(file)
    assert parser, "No parser was associated"
    parser = parser.replace("_format", "")
    assert parser, f"Test file {file} doesn't match any parser"
    assert all(
        item.lower() in file.lower() for item in re.split(r"\.|_", parser)
    ), f"Parser wasn't match to the right parser: {parser}"


onset_file = (
    "tests/parsers_test_files/onset/tidbit_v2/QU5_Mooring_60m_20392474_20220222.csv"
)


@pytest.mark.parametrize(
    "file_path,parser",
    [(onset_file, None), (onset_file, "onset.csv"), (onset_file, onset.csv)],
)
def test_read_file_parser_inputs(file_path, parser):
    """Test if read.file can accept parsers as None, string and parser it self"""
    dataset = read.file(file_path, parser=parser)
    assert isinstance(dataset, Dataset), "Output isn't an xarray dataset"


@pytest.mark.parametrize(
    "file_path,parser",
    [("tests/parsers_test_files/seabird/btl/MI18MHDR.btl", "seabird.btl")],
)
def test_read_file_unique_import(file_path, parser, caplog):
    """Test that read.file only import the parser once"""
    parser_module = parser.rsplit(".")[0]
    with caplog.at_level(logging.DEBUG):
        read.file(file_path, parser=parser)
        assert (
            f"Import module: ocean_data_parser.parsers.{parser_module}" in caplog.text
        ), f"Failed to match log message for module import: {caplog.text}"
        caplog.clear()
        read.file(file_path, parser=parser)
        assert (
            f"Import module: ocean_data_parser.parsers.{parser_module}"
            not in caplog.text
        ), f"Module was imported again: {caplog.text}"
        assert (
            f"Module already imported: ocean_data_parser.parsers.{parser_module}"
            in caplog.text
        ), f"Failed to match log message for module already imported: {caplog.text}"
