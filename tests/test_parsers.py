from glob import glob

import pandas as pd
import pytest
import xarray as xr
from loguru import logger

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


def review_parsed_dataset(ds, source, caplog=None, max_log_levelno=30):
    assert isinstance(ds, xr.Dataset)
    assert ds.attrs, "dataset do not contains any global attributes"
    assert ds.variables, "Dataset has no variables."
    if caplog:
        for record in caplog.records:
            assert record.levelno <= max_log_levelno, str(record) % record.args

    ds.to_netcdf(source + "_test.nc", format="NETCDF4")


@pytest.fixture
def caplog(caplog):
    handler_id = logger.add(caplog.handler, format="{message}")
    yield caplog
    logger.remove(handler_id)


class TestPMEParsers:
    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/pme/**/*.txt", recursive=True)
    )
    def test_txt_parser(self, path, caplog):
        ds = pme.minidot_txt(path)
        review_parsed_dataset(ds, path, caplog)


class TestSeabirdParsers:
    @pytest.mark.parametrize("path", glob("tests/parsers_test_files/seabird/**/*.btl"))
    def test_btl_parser(self, path, caplog):
        ds = seabird.btl(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize("path", glob("tests/parsers_test_files/seabird/**/*.cnv"))
    def test_cnv_parser(self, path, caplog):
        ds = seabird.cnv(path)
        review_parsed_dataset(ds, path, caplog)


class TestVanEssenParsers:
    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/van_essen_instruments/ctd_divers/*.MON")
    )
    def test_mon_parser(self, path, caplog):
        ds = van_essen_instruments.mon(path)
        review_parsed_dataset(ds, path, caplog)


class TestOnsetParser:
    @pytest.mark.parametrize("path", glob("tests/parsers_test_files/onset/**/*.csv"))
    def test_csv_parser(self, path, caplog):
        ds = onset.csv(path)
        review_parsed_dataset(ds, path, caplog)


class TestRBRParser:
    @pytest.mark.parametrize("path", glob("tests/parsers_test_files/rbr/rtext/*.txt"))
    def test_reng_parser(self, path, caplog):
        ds = rbr.rtext(path)
        review_parsed_dataset(ds, path, caplog)


class TestSunburstParsers:
    @pytest.mark.parametrize(
        "path",
        [
            path
            for path in glob("tests/parsers_test_files/sunburst/superCO2/*pCO2*.txt")
            if "_notes_" not in path
        ],
    )
    def test_sunburst_pCO2_parser(self, path, caplog):
        ds = sunburst.superCO2(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/sunburst/superCO2/*pCO2_notes*.txt")
    )
    def test_sunburst_pCO2_notes_parser(self, path, caplog):
        ds = sunburst.superCO2_notes(path)
        review_parsed_dataset(ds, path, caplog)


class TestNMEAParser:
    @pytest.mark.parametrize(
        "path",
        [
            path
            for path in glob("tests/parsers_test_files/nmea/**/*")
            if not path.endswith(".nc")
        ],
    )
    def test_all_files_in_nmea(self, path, caplog):
        ds = nmea.nmea_0183(path)
        review_parsed_dataset(ds, path, caplog)


class TestAmundsenParser:
    @pytest.mark.parametrize(
        "path",
        [
            path
            for path in glob(
                "tests/parsers_test_files/amundsen/**/*.int", recursive=True
            )
            if not path.endswith("info.int")
        ],
    )
    def test_amundsen_int_parser(self, path, caplog):
        ds = amundsen.int_format(path)
        review_parsed_dataset(ds, path, caplog)


class TestIOSShellParser:
    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/ANE/*.ane")
    )
    def test_dfo_ios_shell_ane_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/BOT/*.bot")
    )
    def test_dfo_ios_shell_cruise_bot_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/CHE/*.che")
    )
    def test_dfo_ios_shell_cruise_che_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/CTD/*.ctd")
    )
    def test_dfo_ios_shell_cruise_ctd_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/LOOP/*.loop")
    )
    def test_dfo_ios_shell_cruise_loop_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/MED/*.MED")
    )
    def test_dfo_ios_shell_cruise_med_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/TOB/*.tob")
    )
    def test_dfo_ios_shell_cruise_tob_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/UBC/*.UBC")
    )
    def test_dfo_ios_shell_cruise_ubc_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/DRF/*.drf")
    )
    def test_dfo_ios_shell_drf_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/mooring/CTD/*.ctd")
    )
    def test_dfo_ios_shell_moored_ctd_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/mooring/CUR/*.cur")
    )
    def test_dfo_ios_shell_moored_cur_files(self, path):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path)


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
        "path", glob("tests/parsers_test_files/dfo/odf/bio/**/CTD*.ODF", recursive=True)
    )
    def test_bio_odf_ctd_parser(self, path, caplog):
        """Test DFO BIO ODF Parser"""
        ds = dfo.odf.bio_odf(path)
        review_parsed_dataset(ds, path, caplog)


class TestODFMLIParser:
    @pytest.mark.parametrize(
        "path",
        [
            file
            for datatype in ("BOTL", "BT", "CTD")
            for file in glob(
                f"tests/parsers_test_files/dfo/odf/mli/**/{datatype}*.ODF",
                recursive=True,
            )
        ],
    )
    def test_mli_profile_odf_parser(self, path, caplog):
        """Test DFO BIO ODF Parser"""
        ds = dfo.odf.mli_odf(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path",
        [
            file
            for datatype in ("MCM", "MCTD", "MMOB", "MTC", "MTG", "MTR")
            for file in glob(
                f"tests/parsers_test_files/dfo/odf/mli/**/{datatype}*.ODF",
                recursive=True,
            )
        ],
    )
    def test_mli_timeseries_odf_parser(self, path, caplog):
        """Test DFO BIO ODF Parser"""
        ds = dfo.odf.mli_odf(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path",
        [
            file
            for datatype in ("TCTD", "TSG")
            for file in glob(
                f"tests/parsers_test_files/dfo/odf/mli/**/{datatype}*.ODF",
                recursive=True,
            )
        ],
    )
    def test_mli_trajectory_odf_parser(self, path, caplog):
        """Test DFO BIO ODF Parser"""
        ds = dfo.odf.mli_odf(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path",
        glob(
            "tests/parsers_test_files/dfo/odf/mli/**/MADCP*.ODF",
            recursive=True,
        ),
    )
    def test_mli_madcp_odf_parser(self, path, caplog):
        """Test DFO BIO ODF Parser"""
        ds = dfo.odf.mli_odf(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path",
        glob(
            "tests/parsers_test_files/dfo/odf/mli/**/PLNKG*.ODF",
            recursive=True,
        ),
    )
    def test_mli_plnkg_odf_parser(self, path, caplog):
        """Test DFO BIO ODF Parser"""
        ds = dfo.odf.mli_odf(path)
        review_parsed_dataset(ds, path, caplog)


class TestDFO_NAFC_PcnvFiles:
    @pytest.mark.parametrize(
        "path",
        [
            file
            for file in glob(
                "tests/parsers_test_files/dfo/nafc/pcnv/ctd/*.pcnv",
                recursive=True,
            )
            if not file.endswith(".nc")
        ],
    )
    def test_dfo_nafc_ctd_pcnv(self, path, caplog):
        """Test DFO NAFC Pcnv Parser"""
        ds = dfo.nafc.pcnv(path)
        review_parsed_dataset(ds, path, caplog)


# pylint: disable=W0212
class TestDFO_NAFC_pFiles:
    @pytest.mark.parametrize(
        "path",
        [
            file
            for file in glob(
                "tests/parsers_test_files/dfo/nafc/pfile/**/*.p*",
                recursive=True,
            )
            if not file.endswith(".nc")
        ],
    )
    def test_dfo_nafc_pfile(self, path, caplog):
        """Test DFO BIO ODF Parser"""
        ds = dfo.nafc.pfile(path)
        review_parsed_dataset(ds, path, caplog)

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
        assert response
        assert f"Failed to convert: <{line_parser}" in caplog.text


class TestDfoIosShell:
    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/CTD/*.ctd")
    )
    def test_ios_shell_cruise_ctd_parser(self, path, caplog):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/BOT/*.bot")
    )
    def test_ios_shell_cruise_bot_parser(self, path, caplog):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/CHE/*.che")
    )
    def test_ios_shell_cruise_che_parser(self, path, caplog):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/cruise/TOB/*.tob")
    )
    def test_ios_shell_cruise_tob_parser(self, path, caplog):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/mooring/CTD/*.ctd")
    )
    def test_ios_shell_mooring_ctd_parser(self, path, caplog):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/mooring/CUR/*.CUR")
    )
    def test_ios_shell_mooring_cur_parser(self, path, caplog):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/dfo/ios/shell/DRF/*.drf")
    )
    def test_ios_shell_drifter_parser(self, path, caplog):
        ds = dfo.ios.shell(path)
        review_parsed_dataset(ds, path, caplog)


class TestBlueElectricParse:
    @pytest.mark.parametrize(
        "path",
        [
            path
            for path in glob("tests/parsers_test_files/electricblue/*.csv")
            if "/log" not in path
        ],
    )
    def test_blue_electric_csv_parser(self, path, caplog):
        ds = electricblue.csv(path)
        review_parsed_dataset(ds, path, caplog)

    @pytest.mark.parametrize(
        "path", glob("./tests/parsers_test_files/electricblue/log*.csv", recursive=True)
    )
    def test_blue_electric_log_csv_parser(self, path, caplog):
        ds = electricblue.log_csv(path)
        review_parsed_dataset(ds, path, caplog)


class TestStarOddiParsers:
    @pytest.mark.parametrize(
        "path", glob("tests/parsers_test_files/star_oddi/**/*.DAT", recursive=True)
    )
    def test_star_oddi_dat_parser(self, path, caplog):
        ds = star_oddi.DAT(path)
        review_parsed_dataset(ds, path, caplog)
