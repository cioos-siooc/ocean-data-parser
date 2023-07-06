import difflib
import io
import logging
import re
import unittest
from glob import glob

import pandas as pd
import pytest
import xarray as xr

from ocean_data_parser.read import (
    amundsen,
    auto,
    dfo,
    electricblue,
    nmea,
    onset,
    pme,
    rbr,
    seabird,
    star_oddi,
    sunburst,
    utils,
    van_essen_instruments,
    auto,
    utils,
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


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


def compare_test_to_reference_netcdf(test: xr.Dataset, reference: xr.Dataset):
    def standardize_attributes(value):
        if isinstance(value, str):
            value = value.strip()
            if re.match(
                r"\d\d\d\d-\d\d-\d\d(T|\s)\d\d:\d\d:\d\d(\.\d*){0,1}(Z|[+-]\d\d\:\d\d){0,1}$",
                value,
            ):
                value = pd.to_datetime(value)
        return value

    def standardize_dataset(ds):
        ds.attrs = {
            key: standardize_attributes(value) for key, value in ds.attrs.items()
        }
        for var in ds:
            ds[var].attrs = {
                key: standardize_attributes(value)
                for key, value in ds[var].attrs.items()
            }
        return ds

    def ignore_from_attr(attr, expression, placeholder):
        """Replace expression in both reference and test files which are
        expected to be different."""
        reference.attrs[attr] = re.sub(expression, placeholder, reference.attrs[attr])
        test.attrs[attr] = re.sub(expression, placeholder, test.attrs[attr])

    # Drop some expected differences
    # Add placeholders to specific fields in attributes
    ignore_from_attr(
        "history",
        r"cioos_data_trasform.odf_transform V \d+\.\d+.\d+",
        "cioos_data_trasform.odf_transform V VERSION",
    )
    ignore_from_attr(
        "history", r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.*\d*Z", "TIMESTAMP"
    )
    ignore_from_attr("source", ".*", "source")

    reference.attrs["date_created"] = "TIMESTAMP"
    test.attrs["date_created"] = "TIMESTAMP"

    reference = standardize_dataset(reference)
    test = standardize_dataset(test)

    # Compare only attributes that exist in reference
    test.attrs = {
        attr: value for attr, value in test.attrs.items() if attr in reference.attrs
    }
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


class TestCompareDatasets:
    def _get_test_and_reference(self):
        reference_files = glob(
            "tests/parsers_test_files/**/*_reference.nc", recursive=True
        )
        assert reference_files, "Fail to retrieve any reference netcdf file"
        reference = xr.open_dataset(reference_files[0])
        return reference, reference.copy()

    def _compare_datasets(self, ref, test):
        return compare_xarray_datasets(
            ref, test, fromfile="reference", tofile="test", n=0
        )

    def test_compare_test_to_reference_datasets(self):
        reference, test = self._get_test_and_reference()
        reference.attrs["title"] = "This is the reference title"
        test = reference.copy()
        test.attrs["title"] = "This is the test title"
        differences = self._compare_datasets(reference, test)
        assert differences, "Failed to detect any differences"
        assert len(differences) == 5, "Failed to detect the rigth differences"
        assert any(
            "title" in line for line in differences
        ), "Didn't suggest that the title attribute is changed"


class PMEParserTests(unittest.TestCase):
    def test_txt_parser(self):
        paths = glob("tests/parsers_test_files/pme")
        pme.minidot_txts(paths)


class SeabirdParserTests(unittest.TestCase):
    def test_btl_parser(self):
        paths = glob("tests/parsers_test_files/seabird/*.btl")
        for path in paths:
            seabird.btl(path)

    def test_cnv_parser(self):
        paths = glob("tests/parsers_test_files/seabird/*.cnv")
        for path in paths:
            seabird.cnv(path)


class VanEssenParserTests(unittest.TestCase):
    def test_mon_parser(self):
        paths = glob("tests/parsers_test_files/van_essen_instruments/ctd_divers/*.MON")
        for path in paths:
            van_essen_instruments.mon(path)


class OnsetParserTests(unittest.TestCase):
    def test_csv_parser(self):
        paths = glob("tests/parsers_test_files/onset/**/*.csv")
        for path in paths:
            onset.csv(path)


class RBRParserTests(unittest.TestCase):
    def test_reng_parser(self):
        paths = glob("tests/parsers_test_files/rbr/*.txt")
        for path in paths:
            rbr.rtext(path)


class SunburstParserTests(unittest.TestCase):
    def test_sunburst_pCO2_parser(self):
        paths = glob("tests/parsers_test_files/sunburst/*pCO2*.txt")
        # Ignore note files
        paths = [path for path in paths if "_notes_" not in path]
        for path in paths:
            sunburst.superCO2(path)

    def test_sunburst_pCO2_notes_parser(self):
        paths = glob("tests/parsers_test_files/sunburst/*pCO2_notes*.txt")
        for path in paths:
            sunburst.superCO2_notes(path)


class NMEAParserTest(unittest.TestCase):
    def test_all_files_in_nmea(self):
        paths = glob("tests/parsers_test_files/nmea/*.*")
        for path in paths:
            nmea.file(path)


class AmundsenParserTests(unittest.TestCase):
    def test_amundsen_int_parser(self):
        """Test conversion of int files to xarray."""
        paths = glob("tests/parsers_test_files/amundsen/**/*.int", recursive=True)
        for path in paths:
            if path.endswith("info.int"):
                continue
            amundsen.int_format(path)

    def test_amundsen_int_parser_to_netcdf(self):
        """Test conversion of int files to xarray and netcdf files."""
        paths = glob("tests/parsers_test_files/amundsen/**/*.int", recursive=True)
        for path in paths:
            if path.endswith("info.int"):
                continue
            ds = amundsen.int_format(path)
            ds.to_netcdf(f"{path}_test.nc", format="NETCDF4_CLASSIC")

    def test_amundsen_trajectory_int_parser_to_netcdf(self):
        """Test conversion of trajectory int files to xarray and netcdf files."""
        paths = glob(
            "tests/parsers_test_files/amundsen/*trajectory/**/*.int", recursive=True
        )
        for path in paths:
            if path.endswith("info.int"):
                continue
            ds = amundsen.int_format(path)
            ds.to_netcdf(f"{path}_test.nc", format="NETCDF4_CLASSIC")


class TestODFBIOParser:
    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
    )
    def test_bio_odf_parser(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.bio_odf(file, config=None)

    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
    )
    def test_bio_odf_parser_to_netcdf(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.bio_odf(file, config=None, output="netcdf")

    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
    )
    def test_bio_odf_netcdf(self, file):
        """Test DFO BIO ODF Parser"""
        ds = dfo.odf.bio_odf(file, config=None)
        ds.to_netcdf(f"{file}_test.nc")


class TestODFMLIParser(object):
    @pytest.mark.parametrize(
        "file",
        glob(
            "tests/parsers_test_files/dfo/odf/mli/**/*.ODF_reference.nc", recursive=True
        ),
    )
    def test_mli_all_odf_parser(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file, config=None)

    @pytest.mark.parametrize(
        "file",
        [
            file
            for datatype in ["MCM", "MCTD", "MMOB", "MTC", "MTG", "MTR", "TCTD"]
            for file in glob(
                f"tests/parsers_test_files/dfo/odf/mli/**/{datatype}*.ODF",
                recursive=True,
            )
        ],
    )
    def test_mli_odf_parser_timeseries(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file, config=None)

    @pytest.mark.parametrize(
        "file",
        glob(
            "tests/parsers_test_files/dfo/odf/mli/**/TSG*.ODF",
            recursive=True,
        ),
    )
    def test_mli_odf_parser_trajectory(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file, config=None)

    @pytest.mark.parametrize(
        "file",
        glob(
            "tests/parsers_test_files/dfo/odf/mli/**/MADCP*.ODF",
            recursive=True,
        ),
    )
    def test_mli_odf_parser_madcp(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file)

    @pytest.mark.parametrize(
        "file",
        glob(
            "tests/parsers_test_files/dfo/odf/mli/**/PLNKG*.ODF",
            recursive=True,
        ),
    )
    def test_mli_odf_parser_plnkg(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file)

    @pytest.mark.parametrize(
        "file",
        glob(
            "tests/parsers_test_files/dfo/odf/mli/**/*.ODF",
            recursive=True,
        ),
    )
    def test_mli_odf_netcdf(self, file):
        """Test DFO BIO ODF Parser"""
        ds = dfo.odf.mli_odf(file, config=None)
        ds.to_netcdf(f"{file}_test.nc")


class TestDFOpFiles:
    @pytest.mark.parametrize(
        "file",
        [
            file
            for file in glob(
                "tests/parsers_test_files/dfo/odf/p/**/*.p*",
                recursive=True,
            )
            if not file.endswith(".nc")
        ],
    )
    def test_dfo_nl_p(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.p.parser(file)

    def test_ship_code_mapping(self):
        """Test ship code mapping"""
        response = dfo.p._get_ship_code_metadata(55)
        assert isinstance(response, dict)
        assert response["platform_name"] == "Discovery"

    def test_unknown_ship_code_mapping(self):
        """Test unknown ship code mapping"""
        response = dfo.p._get_ship_code_metadata(9999)
        assert isinstance(response, dict)
        assert "platform_name" not in response

    @pytest.mark.parametrize(
        "line",
        [
            "56001001  47 32.80 -052 35.20 2022-04-10 14:06 0176 S1460 001 V S27-01         1"
        ],
    )
    def test_p_file_metadata_parser_line1(self, line):
        response = dfo.p._parse_pfile_header_line1(line)
        assert isinstance(response, dict)

    @pytest.mark.parametrize(
        "line",
        [
            "56001001 002260  8.00 A 13 #PTCSMOFLHXAW-------            D 000 0001 0173 000 4"
        ],
    )
    def test_p_file_metadata_parser_line2(self, line):
        response = dfo.p._parse_pfile_header_line2(line)
        assert isinstance(response, dict)

    @pytest.mark.parametrize(
        "line",
        [
            "56001001 7 08 02    0999.1 003.8       08 01 18 10 01                          8"
        ],
    )
    def test_p_file_metadata_parser_line3(self, line):
        response = dfo.p._parse_pfile_header_line3(line)
        assert isinstance(response, dict)


class BlueElectricParsertest(unittest.TestCase):
    def test_blue_electric_csv_parser(self):
        paths = glob(
            "./tests/parsers_test_files/electric_blue/**/[!log_]*.csv", recursive=True
        )

        for path in paths:
            ds = electricblue.csv(path)
            ds.to_netcdf(path + "_test.nc")

    def test_blue_electric_log_csv_parser(self):
        paths = glob(
            "./tests/parsers_test_files/electric_blue/**/log*.csv", recursive=True
        )
        for path in paths:
            ds = electricblue.log_csv(path)
            ds.to_netcdf(path + "_test.nc")


class StarOddiParsertest(unittest.TestCase):
    def test_star_oddi_dat_parser(self):
        paths = glob("tests/parsers_test_files/star_oddi/**/*.DAT", recursive=True)
        for path in paths:
            ds = star_oddi.DAT(path)


@pytest.mark.parametrize(
    "reference_file",
    glob("tests/parsers_test_files/**/*_reference.nc", recursive=True),
)
def test_compare_test_to_reference_netcdf(reference_file):
    """Test DFO BIO ODF conversion to NetCDF vs reference files"""
    # Generate test bio odf netcdf files

    # Run Test conversion
    source_file = reference_file.replace("_reference.nc", "")
    test = utils.standardize_dataset(auto.file(source_file))

    # Load test file and reference file
    ref = xr.open_dataset(reference_file)
    difference_detected = compare_test_to_reference_netcdf(test, ref)
    assert (
        not difference_detected
    ), f"Converted file {source_file} is different than the reference: " + "\n".join(
        difference_detected
    )
