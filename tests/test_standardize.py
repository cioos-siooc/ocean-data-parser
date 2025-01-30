import json

import numpy as np
import pandas as pd
import pytest

from ocean_data_parser.parsers import utils


@pytest.mark.parametrize(
    ("value", "expected"),
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


@pytest.mark.parametrize(
    ("value", "dtype", "expected_value"),
    [
        ("str", str, "str"),
        (123, int, 123),
        (123.0, float, 123.0),
        (True, str, "True"),
        (False, str, "False"),
        (pd.Timestamp("2021-01-01"), str, "2021-01-01T00:00:00"),
        (pd.NA, None, None),
        (None, None, None),
        ([], None, None),
        ({}, None, None),
        (np.nan, None, None),
        (pd.NaT, None, None),
        ([1, 4, 5], np.ndarray, np.array([1, 4, 5])),
        ([2.12, 23.2, 0.122], np.ndarray, np.array([2.12, 23.2, 0.122])),
        ([True, False, True], np.ndarray, np.array([True, False, True])),
        ([2, 3, 2.234], np.ndarray, np.array([2, 3, 2.234])),
        ({"a": 1, "b": 2}, str, json.dumps({"a": 1, "b": 2})),
        (
            [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            str,
            json.dumps([{"a": 1, "b": 2}, {"a": 3, "b": 4}]),
        ),
    ],
)
def test_standardize_attribute(value, dtype, expected_value):
    """Test standardize_attributes function."""
    response = utils.standardize_attributes({"test": value})
    assert dtype is None or isinstance(response["test"], dtype), (
        "Attribute was not converted to expected dtype"
    )
    assert "test" not in response if dtype is None else True, (
        "Null attribute was not removed"
    )
    is_equal = response.get("test") == expected_value
    assert all(is_equal) if isinstance(expected_value, np.ndarray) else is_equal, (
        "Attribute was not converted to expected value"
    )
