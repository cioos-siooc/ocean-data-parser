import logging
from glob import glob

import pytest

from ocean_data_parser.parsers import amundsen, onset, seabird, van_essen_instruments

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def batch_parse_and_save_to_netcdf(parser, files):
    for file in files:
        ds = parser(file)
        ds.to_netcdf(f"{file}_test.nc")


def test_benchmark_amundsen_trajectory(benchmark):
    benchmark(
        batch_parse_and_save_to_netcdf,
        parser=amundsen.int_format,
        files=glob(
            "tests/parsers_test_files/amundsen/*trajectory/**/*.int", recursive=True
        ),
    )


def test_benchmark_onset_csv(benchmark):
    benchmark(
        batch_parse_and_save_to_netcdf,
        parser=onset.csv,
        files=glob("tests/parsers_test_files/onset/**/*.cnv", recursive=True),
    )


def test_benchmark_van_essen_mon(benchmark):
    benchmark(
        batch_parse_and_save_to_netcdf,
        parser=van_essen_instruments.mon,
        files=glob(
            "tests/parsers_test_files/van_essen_instruments/**/*.MON", recursive=True
        ),
    )


def test_benchmark_seabird_cnv(benchmark):
    benchmark(
        batch_parse_and_save_to_netcdf,
        parser=seabird.cnv,
        files=glob("tests/parsers_test_files/seabird/**/*.cnv", recursive=True),
    )


def test_benchmark_seabird_btl(benchmark):
    benchmark(
        batch_parse_and_save_to_netcdf,
        parser=seabird.btl,
        files=glob("tests/parsers_test_files/seabird/**/*.btl", recursive=True),
    )
