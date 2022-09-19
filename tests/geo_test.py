import logging
import unittest
from glob import glob

from ocean_data_parser import geo

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

geojson_files = glob("./tests/parsers_test_files/geojson/**/*.geojson", recursive=True)


class StationTests(unittest.TestCase):
    def test_nearest_station(self):
        stations = (("first", 50, -120), ("second", 70, -120))
        nearest = geo.get_nearest_station(52, -120, stations=stations)
        assert nearest == "first", "Wrong nearest station was selected"


class GeoJSONTests(unittest.TestCase):
    def test_geojson_parser(self):
        collections = [geo.read_geojson(file) for file in geojson_files]

    def test_geo_code(self):
        # parse test files
        collections = [geo.read_geojson(file) for file in geojson_files]
        lat, lon = 48.77228044489474, -62.36630494246806  # south of Anticosti Island
        data = geo.get_geo_code(lat, lon, collections)
