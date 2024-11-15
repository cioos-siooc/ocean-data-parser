import unittest
from pathlib import Path

import pandas as pd

from ocean_data_parser.parsers import seabird
from ocean_data_parser.process.process import xr

MODULE_PATH = Path(__file__).parent
TEST_SEABIRD_FILE = (
    MODULE_PATH
    / ".."
    / "tests"
    / "parsers_test_files"
    / "seabird"
    / "mctd"
    / "SBE37SMP-ODO-SDI12_03723671_2023_05_17_datcnv.cnv"
)

qartod_test_config = {
    "sal00": {
        "qartod": {
            "gross_range_test": {
                "suspect_span": [5, 38],
                "fail_span": [2, 41],
            },
            "spike_test": {"suspect_threshold": 0.3, "fail_threshold": 0.9},
            "rate_of_change_test": {"threshold": 0.001},
            "flat_line_test": {
                "tolerance": 0.001,
                "suspect_threshold": 10800,
                "fail_threshold": 21600,
            },
        }
    },
    "prdM": {
        "qartod": {
            "gross_range_test": {
                "suspect_span": [90, 100],
                "fail_span": [0, 200],
            },
        }
    },
}


def load_test_dataset():
    """Load seabird cnv and apply conversion.

    Args:
        file (str): _description_

    Returns:
        _type_: _description_
    """
    ds = seabird.cnv(TEST_SEABIRD_FILE)
    ds["time"] = (
        ds["timeK"].dims,
        pd.to_datetime(ds["timeK"], origin="2000-01-01", unit="s"),
    )
    ds.process.time = "time"
    ds.process.lat = "latitude"
    ds.process.lon = "longitude"
    ds.process.z = "prdM"
    return ds.swap_dims({"index": "time"}).drop_vars("index")


class ProcessLoadDatasetTests(unittest.TestCase):
    def test_test_file_load(self):
        ds = load_test_dataset()
        assert isinstance(ds, xr.Dataset)


test_convention = "tests/{ds.attrs['instrument_type'].strip()}_{ds['time'].min().dt.strftime('%Y%m%d').values}"


class ProcessNamingConvention(unittest.TestCase):
    def test_basic_convention(self):
        ds = load_test_dataset()
        ds.process.filename_convention = test_convention
        filename = ds.process.get_filename_from_convention()
        assert filename == "tests/SBE37SMP-ODO-SDI12_20220510"

    def test_basic_convention_suffix(self):
        ds = load_test_dataset()
        ds.process.filename_convention = test_convention
        filename = ds.process.get_filename_from_convention(suffix="_process_test")
        assert filename == "tests/SBE37SMP-ODO-SDI12_20220510_process_test"


class ProcessSaveNetcdfTests(unittest.TestCase):
    def test_process_save_netcdf(self):
        ds = load_test_dataset()
        ds.process.filename_convention = test_convention
        ds.process.to_netcdf(suffix="_process_test")
        filename = ds.process.get_filename_from_convention(suffix="_process_test.nc")
        assert Path(filename).exists()


class ProcessDatasetDeploymentTests(unittest.TestCase):
    def test_detect_deployment_period(self):
        ds = load_test_dataset()
        ds = ds.process.keep_deployment_period(
            depth="prdM",
            time="time",
        )
        assert isinstance(ds, xr.Dataset)
        assert "deployment_flag" not in ds
        assert ds["time"].min() == pd.to_datetime(
            "2022-05-10T22:00:01"
        ), "Failed to crop te start up to the 8th record"
        assert ds["time"].max() == pd.to_datetime(
            "2023-05-14T20:30:31"
        ), "Failed to crop the end after the 17717th record"


class ProcessGSWTests(unittest.TestCase):
    def test_gsw_z_from_p(self):
        ds = load_test_dataset()
        ds = ds.process.gsw(
            "z_from_p",
            gsw_args=("prdM", 75),
            extra_attrs={"long_name": "Depth"},
        )
        assert (
            "height_above_mean_sea_level" in ds
        ), "height_above_mean_sea_level variable was not generated"
        assert isinstance(ds["height_above_mean_sea_level"], xr.DataArray)
        assert ds["height_above_mean_sea_level"].min().round() == -94

    def test_gsw_z_from_p_to_depth_ufunc(self):
        ds = load_test_dataset()
        extra_attrs = {"long_name": "depth", "standard_name": "depth", "units": "m"}
        ds = ds.process.gsw(
            "z_from_p",
            gsw_args=("prdM", 75),
            ufunc=lambda x: -1 * x,
            extra_attrs=extra_attrs,
        )
        assert isinstance(ds["depth"], xr.DataArray)
        assert ds["depth"].attrs == extra_attrs
        assert ds["depth"].max().round() == 94


class ProcessQartodTests(unittest.TestCase):
    def test_qartod_with_agg(self):
        ds = load_test_dataset()
        ds = ds.process.qartod(
            qartod_test_config,
            agg={
                "sal00_flag": {
                    "tests": [
                        ("sal00", "qartod", "gross_range_test"),
                        ("sal00", "qartod", "spike_test"),
                        ("sal00", "qartod", "rate_of_change_test"),
                        ("sal00", "qartod", "flat_line_test"),
                    ],
                    "streams": ["sal00"],
                }
            },
        )
        assert isinstance(ds, xr.Dataset)
        assert "sal00_flag" in ds
        assert (
            len(ds["sal00"].attrs.get("ancillary_variables", "").split(" ")) == 1
        ), "sal00 ancillary_variables attribute wasn't appropriately generated"
        assert all(
            var in ds
            for var in ds["sal00"].attrs.get("ancillary_variables", "").split(" ")
        ), "not all the flag variables were made available"

    def test_qartod_with_all(self):
        ds = load_test_dataset()
        ds = ds.process.qartod(qartod_test_config, agg="all")
        assert isinstance(ds, xr.Dataset)
        assert "sal00_flag" not in ds
        assert (
            len([var for var in ds if "qartod" in var]) == 5
        ), "Missing some flag variables"
        assert (
            len(ds["sal00"].attrs.get("ancillary_variables", "").split(" ")) == 4
        ), "sal00 ancillary_variables attribute wasn't appropriately generated"
        assert all(
            var in ds
            for var in ds["sal00"].attrs.get("ancillary_variables", "").split(" ")
        ), "not all the flag variables were made available"

    def test_drop_flagged_single_flag_variable(self):
        ds = load_test_dataset()
        ds = ds.process.qartod(
            qartod_test_config,
            agg={
                "sal00_flag": {
                    "tests": [
                        ("sal00", "qartod", "gross_range_test"),
                        ("sal00", "qartod", "spike_test"),
                        ("sal00", "qartod", "rate_of_change_test"),
                        ("sal00", "qartod", "flat_line_test"),
                    ],
                    "streams": ["sal00"],
                }
            },
        )
        # Flag a specific record for testing
        test_records = [100, 200, 300]
        assert ~ds["sal00"][test_records].isnull().all(), "test record is already null"
        ds["sal00_flag"][test_records] = 4
        ds = ds.process.drop_flagged_data(flags=[4], drop_flags=True)
        assert isinstance(ds, xr.Dataset)
        assert "sal00_flag" not in ds
        assert (
            len([var for var in ds if "qartod" in var]) == 0
        ), "Flag variables were not dropped"
        assert (
            len(
                [
                    item
                    for item in ds["sal00"]
                    .attrs.get("ancillary_variables", "")
                    .split(" ")
                    if item
                ]
            )
            == 0
        ), "sal00 ancillary_variables attribute wasn't appropriately removed"
        assert (
            ds["sal00"][test_records].isnull().all()
        ), "Flagged record wasn't replaced by NaN"

    def test_drop_flagged_multiple_flag_variables(self):
        ds = load_test_dataset()
        ds = ds.process.qartod(qartod_test_config, agg="all")
        ds = ds.process.drop_flagged_data(flags=[4])

        assert isinstance(ds, xr.Dataset)
