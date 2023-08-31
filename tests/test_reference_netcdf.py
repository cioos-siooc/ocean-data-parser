"""
This test compare the *_reference.nc files made available within the repository
tests folder to the present associated parser.

Any differences observed between the generated xarray 
object and the reference netcdf will raise an issue.
"""

import difflib
import io
import re
from glob import glob

import pandas as pd
import pytest
import xarray as xr

from ocean_data_parser import read


@pytest.mark.parametrize(
    "reference_file",
    glob("tests/parsers_test_files/**/*_reference.nc", recursive=True),
)
def test_compare_test_to_reference_netcdf(reference_file):
    """Test DFO BIO ODF conversion to NetCDF vs reference files"""
    # Generate test bio odf netcdf files

    # Run Test conversion
    source_file = reference_file.replace("_reference.nc", "")
    test = read.file(source_file)

    # Load test file and reference file
    ref = xr.open_dataset(reference_file)
    difference_detected = compare_test_to_reference_netcdf(ref, test)
    assert (
        not difference_detected
    ), f"Converted file {source_file} is different than the reference: " + "\n".join(
        difference_detected
    )


def compare_xarray_datasets(ds1: xr.Dataset, ds2: xr.Dataset, **kwargs) -> list:
    """Compare two xarray.Dataset.info outputs with difflib.unified_diff.

    Args:
        ds1 (xr.Dataset): First dataset
        ds2 (xr.Dataset): Second datset
        **kwargs (optional): difflib.unified_diff **kwargs

    Returns:
        list: List of differences detected by difflib.
    """

    def _get_xarray_dataset_info(ds):
        f = io.StringIO()
        ds.info(f)
        return f.getvalue().split("\n")

    ds1_info = _get_xarray_dataset_info(ds1)
    ds2_info = _get_xarray_dataset_info(ds2)

    return list(difflib.unified_diff(ds1_info, ds2_info, **kwargs))


def compare_test_to_reference_netcdf(
    reference: xr.Dataset, test: xr.Dataset, sort_variables=True
):
    def _standardize_attributes(value):
        if isinstance(value, str):
            value = value.strip()
            if re.match(
                r"\d\d\d\d-\d\d-\d\d(T|\s)\d\d:\d\d:\d\d(\.\d*){0,1}(Z|[+-]\d\d\:\d\d){0,1}$",
                value,
            ):
                value = pd.to_datetime(value)
        return value

    def _standardize_dataset(ds):
        ds.attrs = {
            key: _standardize_attributes(value) for key, value in ds.attrs.items()
        }
        for var in ds:
            ds[var].attrs = {
                key: _standardize_attributes(value)
                for key, value in ds[var].attrs.items()
            }
        return ds

    def ignore_from_attr(attr, expression, placeholder):
        """Replace expression in both reference and test files which are
        expected to be different."""
        if attr not in reference.attrs or attr not in test.attrs:
            reference[attr] = placeholder
            test[attr] = placeholder
            return
        reference.attrs[attr] = re.sub(expression, placeholder, reference.attrs[attr])
        test.attrs[attr] = re.sub(expression, placeholder, test.attrs[attr])

    # Drop some expected differences
    # Add placeholders to specific fields in attributes
    ignore_from_attr(
        "history",
        r"cioos_data_trasform.odf_transform V \d+\.\d+\.\d+|"
        r"ocean_data_parser V \d+\.\d+\.\d+",
        "package_name_version",
    )
    ignore_from_attr(
        "history", r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.*\d*Z", "TIMESTAMP"
    )
    ignore_from_attr("source", ".*", "source")

    reference.attrs["date_created"] = "TIMESTAMP"
    test.attrs["date_created"] = "TIMESTAMP"

    reference = _standardize_dataset(reference)
    test = _standardize_dataset(test)

    # Convert unicode types to object (<U\d)
    for var in test.variables:
        if var in reference and (
            re.fullmatch(r"<U\d+", str(test[var].dtype))
            or re.fullmatch(r"<U\d+", str(reference[var].dtype))
        ):
            reference[var] = reference[var].astype(object)
            test[var] = test[var].astype(object)

    # Reorder variales to make comparison simpler
    if sort_variables:
        # Sort variables
        variables = [
            *[var for var in reference.variables if var in test],
            *[var for var in test.variables if var not in reference],
        ]
        test = test[variables]

        # Sort variable attributes by reference order
        test.attrs = {
            **{
                key: test.attrs[key]
                for key in reference.attrs.keys()
                if key in test.attrs
            },
            **{
                key: value
                for key, value in test.attrs.items()
                if key not in reference.attrs
            },
        }
        for var in test.variables:
            if var not in reference or not reference[var].attrs:
                continue
            test[var].attrs = {
                **{
                    key: test[var].attrs[key]
                    for key in reference[var].attrs.keys()
                    if key in test[var].attrs
                },
                **{
                    key: value
                    for key, value in test.items()
                    if key not in reference[var].attrs
                },
            }

    # Compare only attributes that exist in reference
    # test.attrs = {
    #     **{attr: value for attr, value in test.attrs.items() if attr in reference.attrs},
    #     **{attr: value for attr, value in test.attrs.items() if attr in reference.attrs}
    # }

    for var in test:
        if var not in reference and var in test:
            test = test.drop(var)
            continue
        test[var].attrs = {
            attr: value
            for attr, value in test[var].attrs.items()
            if attr in reference[var].attrs
        }

    for var in test.coords:
        if var not in reference:
            continue
        test[var].attrs = {
            attr: value
            for attr, value in test[var].attrs.items()
            if attr in reference[var].attrs
        }

    if reference.identical(test):
        return []
    differences = compare_xarray_datasets(
        reference, test, fromfile="reference", tofile="test", n=0
    )
    return "Unknown differences" if not differences else differences
