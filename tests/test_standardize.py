import pandas as pd
import pytest

from ocean_data_parser.parsers import utils


@pytest.mark.parametrize(
    "value,expected",
    [
        ("str", True),
        (123, True),
        ([1, 4, 5], True),
        (True, True),
        ([], False),
        ({}, False),
        (pd.NA, False),
        (None, False),
    ],
)
def test_consider_attribute(value, expected):
    response = utils._consider_attribute(value)
    assert response == expected
