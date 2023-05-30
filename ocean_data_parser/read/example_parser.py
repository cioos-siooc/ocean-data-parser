"""
General doc string that describe a standard parser module
"""
# Import Section
# import ...
from pathlib import Path
import logging

import pandas as pd
import xarray as xr

logger = logging.getLogger()

MODULE_PATH = Path(__file__).parent

module_global_attributes = {}
variables_vocabulary = {}


def _add_vocabulary(
    dataset: xr.Dataset, variable: xr.DataArray, add="all"
) -> xr.Dataset:
    def _is_matched():
        pass

    def _get_vocabulary_variable():
        new_variable = variable.copy()
        new_variable = eval(attrs.pop("apply", lambda x: x), {}, {"ds": dataset})
        new_variable.attrs = attrs
        return new_variable

    # TODO add vocabulary matching logic
    matched_vocabularies = _is_matched()
    for name, attrs in matched_vocabularies.items():
        dataset[name] = _get_vocabulary_variable()
    return


def parser(source: str, encoding="UTF-8", optional_inputs=None) -> xr.Dataset:
    def _get_global_attributes_from_header() -> dict:
        return {}

    def _get_variable_attributes_from_header(variable: str) -> dict:
        return {}

    def _convert_time_variables() -> xr.Dataset:
        return ds

    # Parse file
    line = []
    header = {}
    original_header = []
    with open(source, encoding=encoding) as file_handle:
        while file_handle:
            line = file_handle.readline()
            original_header += [line]
            if "END of HEADER" in line:
                break
            # parse line and convert to dictonary
            key, value = line.split("=")
            header[key] = value

        # Parse data section
        # pandas contains a lot method to parse different file format
        # pd.read_csv, pd.read_fwf are often appropriate
        df = pd.read_csv(file_handle)

    # TODO Parse header dictionary further if not already done in while loop

    # Name variables if not already and on convert to xarray dataset
    df.columns = header["columns"]
    ds = df.to_xarray()

    # TODO Convert time variables to datetime
    ds = _convert_time_variables()

    # TODO Add global attributes
    ds.attrs = {
        **module_global_attributes,
        **_get_global_attributes_from_header(),
        "source": source,
        "original_header": original_header,
    }

    # TODO Add variable attributes from header
    for var in ds:
        ds[var].attrs = _get_variable_attributes_from_header()
        ds = _add_vocabulary(ds[var])

    return ds
