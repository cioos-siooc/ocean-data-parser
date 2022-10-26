import logging
import re
import unittest
from glob import glob
import os

import xarray as xr
import pandas as pd
import numpy as np
from ocean_data_parser.read import (
    seabird,
    van_essen_instruments,
    onset,
    amundsen,
    electricblue,
    star_oddi,
    rbr,
    sunburst,
    dfo,
    nmea,
    pme,
)
from ocean_data_parser.read import file

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def compare_test_to_reference_netcdf(files):
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

    def not_identical(ref, test):
        return (ref != test).any() if isinstance(ref, np.ndarray) else ref != test

    def ignore_from_attr(attr, expression, placeholder):
        """Replace expression in both reference and test files which are
        expected to be different."""
        ref.attrs[attr] = re.sub(expression, placeholder, ref.attrs[attr])
        test.attrs[attr] = re.sub(expression, placeholder, test.attrs[attr])

    for file in files:
        ref = xr.open_dataset(file)
        nc_file_test = file.replace("_reference.nc", "_test.nc")
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
            "history", r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.*\d*Z", "TIMESTAMP"
        )
        ref.attrs["date_created"] = "TIMESTAMP"
        test.attrs["date_created"] = "TIMESTAMP"

        ref = standardize_dataset(ref)
        test = standardize_dataset(test)

        if not ref.identical(test):

            # Global attributes
            for key in ref.attrs.keys():
                if not_identical(ref.attrs[key], test.attrs.get(key)):
                    raise RuntimeWarning(
                        f"Different Global attribute detected {key} -> REF {ref.attrs[key]} -> TEST {test.attrs.get(key)} changed"
                    )

            # new global attributes
            new_global_attributes = {
                att: value for att, value in test.attrs.items() if att not in ref.attrs
            }
            if new_global_attributes:
                raise RuntimeWarning(
                    f"Extra attributes dectected that are not in the reference file: {new_global_attributes}"
                )

            # Variables
            for var in ref:
                # Variable Attributes
                for key in ref[var].attrs.keys():
                    if not_identical(ref[var].attrs[key], test[var].attrs.get(key)):
                        raise RuntimeWarning(
                            f"Variable attribute changed {key}[{var}].attrs ->"
                        )
                new_variable_attributes = {
                    att: value
                    for att, value in test[var].attrs.items()
                    if att not in ref[var].attrs
                }
                if new_variable_attributes:
                    raise RuntimeWarning(
                        f"Extra variable attributes dectected that are not in the reference file: {var} -> {new_variable_attributes}"
                    )

                # Values
                if not ref[var].identical(test[var]):
                    raise RuntimeWarning(
                        f"Variable ds[{var}] is different from reference file"
                    )

            raise RuntimeWarning(
                f"Converted file {nc_file_test} is different than the reference: {file}"
            )


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

    def test_cnv_auto_parser(self):
        paths = glob("tests/parsers_test_files/seabird/*.cnv")
        for path in paths:
            file(path)


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


class ODFParsertest(unittest.TestCase):
    def test_bio_odf_parser(self):
        """Test DFO BIO ODF Parser"""
        paths = glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
        for path in paths:
            dfo.odf.bio_odf(path, config=None)

    def test_bio_odf_parser_to_netcdf(self):
        """Test DFO BIO ODF Parser"""
        paths = glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
        for path in paths:
            dfo.odf.bio_odf(path, config=None, output="netcdf")

    def test_mli_all_odf_parser(self):
        """Test DFO BIO ODF Parser"""
        paths = glob("tests/parsers_test_files/dfo/odf/mli/**/*.ODF", recursive=True)
        for path in paths:
            dfo.odf.mli_odf(path, config=None)

    def test_mli_odf_parser_timeseries(self):
        """Test DFO BIO ODF Parser"""
        datatypes = ["MCM", "MCTD", "MMOB", "MTC", "MTG", "MTR", "TCTD"]
        for datatype in datatypes:
            paths = glob(
                f"tests/parsers_test_files/dfo/odf/mli/**/{datatype}*.ODF",
                recursive=True,
            )
            for path in paths:
                dfo.odf.mli_odf(path, config=None)

    def test_mli_odf_parser_trajectory(self):
        """Test DFO BIO ODF Parser"""
        paths = glob(
            "tests/parsers_test_files/dfo/odf/mli/**/TSG*.ODF",
            recursive=True,
        )
        for path in paths:
            dfo.odf.mli_odf(path, config=None)

    def test_mli_odf_parser_madcp(self):
        """Test DFO BIO ODF Parser"""
        paths = glob(
            "tests/parsers_test_files/dfo/odf/mli/**/MADCP*.ODF", recursive=True
        )
        for path in paths:
            dfo.odf.mli_odf(path)

    def test_mli_odf_parser_plnkg(self):
        """Test DFO BIO ODF Parser"""
        paths = glob(
            "tests/parsers_test_files/dfo/odf/mli/**/PLNKG*.ODF", recursive=True
        )
        for path in paths:
            dfo.odf.mli_odf(path)

    def test_bio_odf_netcdf(self):
        """Test DFO BIO ODF Parser"""
        paths = glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
        for path in paths:
            ds = dfo.odf.bio_odf(path, config=None)
            ds.to_netcdf(f"{path}_test.nc")

    def test_mli_odf_netcdf(self):
        """Test DFO BIO ODF Parser"""
        paths = glob("tests/parsers_test_files/dfo/odf/bio/**/*.ODF", recursive=True)
        for path in paths:
            ds = dfo.odf.mli_odf(path, config=None)
            ds.to_netcdf(f"{path}_test.nc")

    def test_bio_odf_converted_netcdf_vs_references(self):
        """Test DFO BIO ODF conversion to NetCDF vs reference files"""
        # Generate test bio odf netcdf files
        self.test_bio_odf_netcdf()

        # Retriev the list of reference files
        ref_files = glob(
            "./tests/parsers_test_files/dfo/odf/bio/**/*.ODF_reference.nc",
            recursive=True,
        )
        compare_test_to_reference_netcdf(ref_files)


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
