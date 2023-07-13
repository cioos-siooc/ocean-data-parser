import logging
import re
from glob import glob
from pathlib import Path

import pytest
from xarray import Dataset

from ocean_data_parser.read import detect_file_format
from ocean_data_parser.read import file as auto_read

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

auto_detection_ignore = [
    "CTD_1994038_147_1_DN.ODF",
    "CTD_2020003_004_1_DN.ODF",
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
    parser = detect_file_format(file)
    assert parser, "No parser was associated"
    parser = parser.replace("_format", "")
    assert parser, f"Test file {file} doesn't match any parser"
    assert all(
        item.lower() in file.lower() for item in re.split(r"\.|_", parser)
    ), f"Parser wasn't match to the right parser: {parser}"


@pytest.mark.parametrize(
    "file",
    [
        str(file)
        for file in Path("tests/parsers_test_files").glob("**/*.*")
        if not file.name.endswith(auto_detection_ignore_extensions)
        and "geojson" not in file.name
    ],
)
def test_detect_and_parse(file):
    dataset = auto_read(file)
    assert isinstance(dataset, Dataset), "Output isn't an xarray dataset"
