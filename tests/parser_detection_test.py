import logging
from pathlib import Path
import re
import unittest
from glob import glob
import pytest

from xarray import Dataset

from ocean_data_parser.read import detect_file_format, file as auto_read

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

auto_detection_ignore = [
    "tests/parsers_test_files/dfo/odf/bio/CTD/CTD_1994038_147_1_DN.ODF",
    "tests/parsers_test_files/dfo/odf/bio/CTD/CTD_2020003_004_1_DN.ODF",
]


@pytest.mark.parametrize(
    "file",
    [
        file
        for file in glob("tests/parsers_test_files/**/*.*", recursive=True)
        if not file.endswith("nc")
        and file not in auto_detection_ignore
    ],
)
def test_automated_parser_detection(file):
    parser = detect_file_format(file)
    parser = parser.replace("_format", "")
    assert parser, f"Test file {file} doesn't match any parser"
    assert all(
        item.lower() in file.lower() for item in re.split(r"\.|_", parser)
    ), f"Parser wasn't match to the right parser: {parser}"


@pytest.mark.parametrize(
    "file",
    [
        file
        for file in glob("tests/parsers_test_files/**/*.*", recursive=True)
        if not file.endswith("nc") and "geojson" not in file and Path(file).exists()
    ],
)
def test_detect_and_parse(file):
    dataset = auto_read(file)
    assert isinstance(dataset, Dataset), "Output isn't an xarray dataset"
