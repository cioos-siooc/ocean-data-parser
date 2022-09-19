import logging
import re
import unittest
from glob import glob
import os

import xarray as xr
from ocean_data_parser import read

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class PMEParserTests(unittest.TestCase):
    def test_txt_parser(self):
        paths = glob("tests/parsers_test_files/pme")
        read.pme.minidot_txts(paths)


class SeabirdParserTests(unittest.TestCase):
    def test_btl_parser(self):
        paths = glob("tests/parsers_test_files/seabird/*.btl")
        for path in paths:
            read.seabird.btl(path)

    def test_cnv_parser(self):
        paths = glob("tests/parsers_test_files/seabird/*.cnv")
        for path in paths:
            read.seabird.cnv(path)


class VanEssenParserTests(unittest.TestCase):
    def test_mon_parser(self):
        paths = glob("tests/parsers_test_files/van_essen_instruments/ctd_divers/*.MON")
        for path in paths:
            read.van_essen_instruments.mon(path)


class OnsetParserTests(unittest.TestCase):
    def test_csv_parser(self):
        paths = glob("tests/parsers_test_files/onset/**/*.csv")
        for path in paths:
            read.onset.csv(path)


class RBRParserTests(unittest.TestCase):
    def test_reng_parser(self):
        paths = glob("tests/parsers_test_files/rbr/*.txt")
        for path in paths:
            read.rbr.rtext(path)


class SunburstParserTests(unittest.TestCase):
    def test_sunburst_pCO2_parser(self):
        paths = glob("tests/parsers_test_files/sunburst/*pCO2*.txt")
        # Ignore note files
        paths = [path for path in paths if "_notes_" not in path]
        for path in paths:
            read.sunburst.superCO2(path)

    def test_sunburst_pCO2_notes_parser(self):
        paths = glob("tests/parsers_test_files/sunburst/*pCO2_notes*.txt")
        for path in paths:
            read.sunburst.superCO2_notes(path)


class NMEAParserTest(unittest.TestCase):
    def test_all_files_in_nmea(self):
        paths = glob("tests/parsers_test_files/nmea/*.*")
        for path in paths:
            read.nmea.file(path)


class AmundsenParserTests(unittest.TestCase):
    def test_amundsen_int_parser(self):
        """Test conversion of int files to xarray."""
        paths = glob("tests/parsers_test_files/amundsen/**/*.int", recursive=True)
        for path in paths:
            if path.endswith("info.int"):
                continue
            read.amundsen.int_format(path)

    def test_amundsen_int_parser_to_netcdf(self):
        """Test conversion of int files to xarray and netcdf files."""
        paths = glob("tests/parsers_test_files/amundsen/**/*.int", recursive=True)
        for path in paths:
            if path.endswith("info.int"):
                continue
            ds = read.amundsen.int_format(path)
            ds.to_netcdf(f"{path}_test.nc", format="NETCDF4_CLASSIC")

    def test_amundsen_trajectory_int_parser_to_netcdf(self):
        """Test conversion of trajectory int files to xarray and netcdf files."""
        paths = glob(
            "tests/parsers_test_files/amundsen/*trajectory/**/*.int", recursive=True
        )
        for path in paths:
            if path.endswith("info.int"):
                continue
            ds = read.amundsen.int_format(path)
            ds.to_netcdf(f"{path}_test.nc", format="NETCDF4_CLASSIC")


class ODFParsertest(unittest.TestCase):
    def test_bio_odf_parser(self):
        """Test DFO BIO ODF Parser"""
        paths = glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
        for path in paths:
            ds = read.dfo.bio_odf(path, config=None)

    def test_mli_odf_parser(self):
        """Test DFO BIO ODF Parser"""
        paths = glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
        for path in paths:
            ds = read.dfo.mli_odf(path, config=None)

    def test_bio_odf_netcdf(self):
        """Test DFO BIO ODF Parser"""
        paths = glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
        for path in paths:
            ds = read.dfo.bio_odf(path, config=None)
            ds.to_netcdf(f"{path}_test.nc")

    def test_mli_odf_netcdf(self):
        """Test DFO BIO ODF Parser"""
        paths = glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
        for path in paths:
            ds = read.dfo.mli_odf(path, config=None)
            ds.to_netcdf(f"{path}_test.nc")

    def test_bio_odf_converted_netcdf_vs_references(self):
        """Test DFO BIO ODF conversion to NetCDF vs reference files"""

        def ignore_from_attr(attr, expression, placeholder):
            """Replace expression in both reference and test files which are expected to be different."""
            ref.attrs[attr] = re.sub(expression, placeholder, ref.attrs[attr])
            test.attrs[attr] = re.sub(expression, placeholder, test.attrs[attr])

        # Run bio odf conversion
        self.test_bio_odf_netcdf()

        # Compare to reference files
        nc_files = glob(
            "./tests/parsers_test_files/dfo/odf/bio/**/*.ODF_reference.nc",
            recursive=True,
        )

        for nc_file in nc_files:
            ref = xr.open_dataset(nc_file)
            nc_file_test = nc_file.replace("_reference.nc", "_test.nc")
            if not os.path.isfile(nc_file_test):
                raise RuntimeError(f"{nc_file_test} was not generated.")
            test = xr.open_dataset(nc_file_test)

            # Add placeholders to specific fields in attributes
            ignore_from_attr(
                "history",
                r"cioos_data_trasform.odf_transform V \d+\.\d+.\d+",
                "cioos_data_trasform.odf_transform V VERSION",
            )
            ignore_from_attr(
                "history", r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ", "TIMESTAMP"
            )
            if "geographic_area" in ref.attrs:
                ref.attrs.pop("geographic_area")
            if "station" in ref.attrs:
                ref.attrs.pop("station")

            ref.attrs["date_created"] = "TIMESTAMP"
            test.attrs["date_created"] = "TIMESTAMP"

            if not ref.identical(test):
                for key, value in ref.attrs.items():
                    if test.attrs.get(key) != value:
                        logger.error(
                            "Global attribute ds.attrs[%s] is different from reference file",
                            key,
                        )
                for var in ref:
                    if not ref[var].identical(test[var]):
                        logger.error(
                            "Variable ds[%s] is different from reference file", var
                        )
                raise RuntimeError(
                    f"Converted file {nc_file_test} is different than the reference: {nc_file}"
                )
