import difflib
import io
import logging
import re
import unittest
from glob import glob

import pandas as pd
import pytest
import xarray as xr

from ocean_data_parser import read
from ocean_data_parser.parsers import (
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
from ocean_data_parser.parsers.dfo.odf_source.attributes import _review_station
from ocean_data_parser.parsers.dfo.odf_source.parser import _convert_odf_time

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


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


class TestODFParser:
    @pytest.mark.parametrize(
        "timestamp,expected_response",
        [
            (None, pd.NaT),
            ("17-NOV-1858 00:00:00.00", pd.NaT),
            (
                "01-Dec-2022 00:00:00",
                pd.Timestamp(
                    year=2022, month=12, day=1, hour=0, minute=0, second=0, tz="UTC"
                ),
            ),
            (
                "1-Dec-2022 01:02:03.123",
                pd.Timestamp(
                    year=2022,
                    month=12,
                    day=1,
                    hour=1,
                    minute=2,
                    second=3,
                    microsecond=123000,
                    tz="UTC",
                ),
            ),
            (
                "01-Dec-2022 00:00:60",
                pd.Timestamp(
                    year=2022, month=12, day=1, hour=0, minute=1, second=0, tz="UTC"
                ),
            ),
        ],
    )
    def test_odf_timestamp_parser(self, timestamp, expected_response):
        response = _convert_odf_time(timestamp)
        assert response is expected_response or response == expected_response

    @pytest.mark.parametrize(
        "timestamp,expected_response",
        [
            ("2022-12-11 00:00:00.00", pd.NaT),
            ("2022-20-20 00:00:00.00", pd.NaT),
            ("2022-20-20", pd.NaT),
        ],
    )
    def test_failed_odf_timestamp_parser(self, timestamp, expected_response, caplog):
        response = _convert_odf_time(timestamp)
        assert response is expected_response or response == expected_response
        assert "Unknown time format" in caplog.text
        assert "Failed to parse the timestamp" in caplog.text

    @pytest.mark.parametrize(
        "original_header,station",
        [
            (" station: QU05", "QU05"),
            ("somethinng station: QU05 some more", "QU05"),
            ("station: QU5 some more", "QU05"),
            ("station: QU_5 some more", "QU05"),
            ("station_name: QU_5 some more", "QU05"),
            ("station: QU_05 some more", "QU05"),
            ("station: 2 ", None),
            ("some text station: ", None),
            ("|some text before 223 ;nom de la station ", "223"),
            ("|some text before 1 ;nom de la station ", "001"),
            ("|some text before QU31 ;nom de la station ", None),
            (";nom de la station ", None),
        ],
    )
    def test_odf_station_search(self, original_header, station):
        response = _review_station({}, {"original_header": original_header})
        assert response == station, f"Failed to retrieve station={station}"

    @pytest.mark.parametrize(
        "global_attributes,original_header,station",
        [
            ({"station": "ABC04"}, "station: DEF05", "ABC04"),
            ({"station": None}, "station: DEF05", "DEF05"),
            ({"station": None}, "no station", None),
            ({"station": "station"}, "some text", "station"),
            ({}, "some text", None),
        ],
    )
    def test_odf_station_in_globals(self, global_attributes, original_header, station):
        response = _review_station(
            global_attributes, {"original_header": original_header}
        )
        assert response == station, f"Failed to retrieve station={station}"


class TestODFBIOParser:
    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/odf/bio/**/CTD*.ODF", recursive=True)
    )
    def test_bio_odf_ctd_parser(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.bio_odf(file).to_netcdf(f"{file}_test.nc")


class TestODFMLIParser:
    @pytest.mark.parametrize(
        "file",
        [
            file
            for datatype in ("BOTL", "BT", "CTD")
            for file in glob(
                f"tests/parsers_test_files/dfo/odf/mli/**/{datatype}*.ODF",
                recursive=True,
            )
        ],
    )
    def test_mli_profile_odf_parser(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file).to_netcdf(f"{file}_test.nc")

    @pytest.mark.parametrize(
        "file",
        [
            file
            for datatype in ("MCM", "MCTD", "MMOB", "MTC", "MTG", "MTR")
            for file in glob(
                f"tests/parsers_test_files/dfo/odf/mli/**/{datatype}*.ODF",
                recursive=True,
            )
        ],
    )
    def test_mli_timeseries_odf_parser(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file).to_netcdf(f"{file}_test.nc")

    @pytest.mark.parametrize(
        "file",
        [
            file
            for datatype in ("TCTD", "TSG")
            for file in glob(
                f"tests/parsers_test_files/dfo/odf/mli/**/{datatype}*.ODF",
                recursive=True,
            )
        ],
    )
    def test_mli_trajectory_odf_parser(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file).to_netcdf(f"{file}_test.nc")

    @pytest.mark.parametrize(
        "file",
        glob(
            "tests/parsers_test_files/dfo/odf/mli/**/MADCP*.ODF",
            recursive=True,
        ),
    )
    def test_mli_madcp_odf_parser(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file).to_netcdf(f"{file}_test.nc")

    @pytest.mark.parametrize(
        "file",
        glob(
            "tests/parsers_test_files/dfo/odf/mli/**/PLNKG*.ODF",
            recursive=True,
        ),
    )
    def test_mli_plnkg_odf_parser(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.odf.mli_odf(file).to_netcdf(f"{file}_test.nc")


class TestDFOpFiles:
    @pytest.mark.parametrize(
        "file",
        [
            file
            for file in glob(
                "tests/parsers_test_files/dfo/nafc/pfile/**/*.p*",
                recursive=True,
            )
            if not file.endswith(".nc")
        ],
    )
    def test_dfo_nafc_pfile(self, file):
        """Test DFO BIO ODF Parser"""
        dfo.nafc.pfile(file)

    def test_ship_code_mapping(self):
        """Test ship code mapping"""
        response = dfo.nafc._get_ship_code_metadata(55)
        assert isinstance(response, dict)
        assert response["platform_name"] == "Discovery"

    def test_unknown_ship_code_mapping(self):
        """Test unknown ship code mapping"""
        response = dfo.nafc._get_ship_code_metadata(9999)
        assert isinstance(response, dict)
        assert "platform_name" not in response

    @pytest.mark.parametrize(
        "line",
        [
            "56001001  47 32.80 -052 35.20 2022-04-10 14:06 0176 S1460 001 V S27-01         1"
        ],
    )
    def test_p_file_metadata_parser_line1(self, line):
        response = dfo.nafc._parse_pfile_header_line1(line)
        assert isinstance(response, dict)

    @pytest.mark.parametrize(
        "line",
        [
            "56001001 002260  8.00 A 13 #PTCSMOFLHXAW-------            D 000 0001 0173 000 4"
        ],
    )
    def test_p_file_metadata_parser_line2(self, line):
        response = dfo.nafc._parse_pfile_header_line2(line)
        assert isinstance(response, dict)

    @pytest.mark.parametrize(
        "line",
        [
            "56001001 7 08 02    0999.1 003.8       08 01 18 10 01                          8"
        ],
    )
    def test_p_file_metadata_parser_line3(self, line):
        response = dfo.nafc._parse_pfile_header_line3(line)
        assert isinstance(response, dict)

    @pytest.mark.parametrize(
        "line_parser",
        [
            "_parse_pfile_header_line1",
            "_parse_pfile_header_line2",
            "_parse_pfile_header_line3",
        ],
    )
    def test_p_file_metadata_parser_line_failed(self, caplog, line_parser):
        parser = getattr(dfo.nafc, line_parser)
        response = parser(
            "56001001 7 08 0a    0999.1 003.8       08 01 18 10 01                          8"
        )
        assert isinstance(response, dict)
        assert not response
        assert f"Failed to parse {line_parser}" in caplog.text


class TestDfoIosShell:
    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/ios/shell/cruise/CTD/*.ctd")
    )
    def test_ios_shell_cruise_ctd_parser(self, file):
        ds = dfo.ios.shell(file)
        assert isinstance(ds, xr.Dataset)

    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/ios/shell/cruise/BOT/*.bot")
    )
    def test_ios_shell_cruise_bot_parser(self, file):
        ds = dfo.ios.shell(file)
        assert isinstance(ds, xr.Dataset)

    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/ios/shell/cruise/CHE/*.che")
    )
    def test_ios_shell_cruise_che_parser(self, file):
        ds = dfo.ios.shell(file)
        assert isinstance(ds, xr.Dataset)

    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/ios/shell/cruise/TOB/*.tob")
    )
    def test_ios_shell_cruise_tob_parser(self, file):
        ds = dfo.ios.shell(file)
        assert isinstance(ds, xr.Dataset)

    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/ios/shell/mooring/CTD/*.ctd")
    )
    def test_ios_shell_mooring_ctd_parser(self, file):
        ds = dfo.ios.shell(file)
        assert isinstance(ds, xr.Dataset)

    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/ios/shell/mooring/CUR/*.CUR")
    )
    def test_ios_shell_mooring_cur_parser(self, file):
        ds = dfo.ios.shell(file)
        assert isinstance(ds, xr.Dataset)

    @pytest.mark.parametrize(
        "file", glob("tests/parsers_test_files/dfo/ios/shell/DRF/*.drf")
    )
    def test_ios_shell_drifter_parser(self, file):
        ds = dfo.ios.shell(file)
        assert isinstance(ds, xr.Dataset)


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
            star_oddi.DAT(path)
