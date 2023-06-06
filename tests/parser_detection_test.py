import logging
from pathlib import Path

import xarray as xr
import pytest

from ocean_data_parser.read import detect_file_format

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

test_files = [
    str(path)
    for path in Path("tests/parsers_test_files").glob("**/*")
    if path.is_file() and path.suffix != ".nc"
]


@pytest.mark.parametrize("file", test_files)
def test_automated_parser_detection_from_file(file):
    parser = detect_file_format(file)
    assert parser, f"Test file {file} doesn't match any parser"


@pytest.mark.parametrize("file", test_files)
def test_automated_parser_detection_and_parsing(file):
    output = file(file)
    assert isinstance(output, xr.Dataset)
