import logging
import os
import re
import unittest
import warnings
from glob import glob

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from ocean_data_parser.read import (
    amundsen,
    dfo,
    electricblue,
    nmea,
    onset,
    pme,
    rbr,
    seabird,
    star_oddi,
    sunburst,
    van_essen_instruments,
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def compare_test_to_reference_netcdf(file):
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
        ref.attrs[attr] = re.sub(expression, placeholder, ref.attrs[attr])
        test.attrs[attr] = re.sub(expression, placeholder, test.attrs[attr])

    # Run Test conversion
    test = dfo.odf.bio_odf(file)
    # Load test file and reference file
    ref = xr.open_dataset(file + "_reference.nc")

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

    ref.attrs["date_created"] = "TIMESTAMP"
    test.attrs["date_created"] = "TIMESTAMP"

    ref = standardize_dataset(ref)
    test = standardize_dataset(test)

    # Compare only attributes that exist in reference
    test.attrs = {
        attr: value for attr, value in test.attrs.items() if attr in ref.attrs
    }
    for var in test:
        if var not in ref:
            test.drop(var)
        test[var].attrs = {
            attr: value
            for attr, value in test[var].attrs.items()
            if attr in ref[var].attrs
        }

    for var in test.coords:
        if var not in ref:
            continue
        test[var].attrs = {
            attr: value
            for attr, value in test[var].attrs.items()
            if attr in ref[var].attrs
        }

    if ref.identical(test):
        return []
    difference_detected = []
    # find through netcdf files differences
    for key, value in ref.attrs.items():
        if test.attrs.get(key) != value:
            difference_detected += [
                f"Global attribute ref.attrs[{key}]={value} != test.attrs[{key}]={test.attrs.get(key)}",
            ]

    if test.attrs.keys() != ref.attrs.keys():
        difference_detected += [
            f"A new global attribute {set(test.attrs.keys()) - set(ref.attrs.keys())} was detected.",
        ]

    ref_variables = list(ref) + list(ref.coords)
    test_variables = list(test) + list(test.coords)

    if ref_variables.sort() != test_variables.sort():
        difference_detected += [
            "A variable mismatch exist between the reference and test files"
        ]

    for var in ref_variables:
        # compare variable
        if not ref[var].identical(test[var]):
            difference_detected += [
                f"Variable ds[{var}] is different from reference file"
            ]
        if (ref[var].values != test[var].values).any():
            difference_detected += [
                f"Variable ds[{var}].values are different from reference file"
            ]

        # compare variable attributes
        for attr, value in ref[var].attrs.items():
            is_not_same_attr = test[var].attrs.get(attr) != value
            if isinstance(is_not_same_attr, bool) and not is_not_same_attr:
                continue
            elif isinstance(is_not_same_attr, bool) and is_not_same_attr:
                difference_detected += [
                    f"Variable Attribute ref[{var}].attrs[{attr}]={value} != test[{var}].attrs[{attr}]={test[var].attrs.get(attr)}",
                ]
            elif (is_not_same_attr).any():
                difference_detected += [
                    f"Variable ds[{var}].attrs[{attr}] is different from reference file",
                ]
    logger.error(
        "Test file differ from reference: %s", "\n + ".join(difference_detected)
    )
    return difference_detected


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

    @pytest.mark.parametrize(
        "file",
        glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True),
    )
    def test_bio_odf_converted_netcdf_vs_references(self, file):
        """Test DFO BIO ODF conversion to NetCDF vs reference files"""
        # Generate test bio odf netcdf files
        difference_detected = compare_test_to_reference_netcdf(file)
        assert (
            not difference_detected
        ), f"Converted file {file} is different than the reference: " + "\n + ".join(
            difference_detected
        )


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
