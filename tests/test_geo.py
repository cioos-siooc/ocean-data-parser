import logging
import unittest
from glob import glob

import pandas as pd
import xarray as xr

from ocean_data_parser import geo

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

geojson_files = ["tests/test-geo.geojson"]

reference_stations = (("first", 50, -120), ("second", 70, -120))


class StationTests(unittest.TestCase):
    def test_nearest_station(self):
        nearest = geo.get_nearest_station(52, -120, stations=reference_stations)
        assert nearest == "first", "Wrong nearest station was selected"

    def test_nearest_station_with_max_distance(self):
        nearest = geo.get_nearest_station(
            52, -120, stations=reference_stations, max_distance_from_station_km=10000
        )
        assert nearest == "first", "Wrong nearest station was selected"

    def test_nearest_station_with_too_far_stations(self):
        nearest = geo.get_nearest_station(
            52, -120, stations=reference_stations, max_distance_from_station_km=1
        )
        assert nearest is None

    def test_nearest_station_with_xarray_dataarray(self):
        ds = xr.Dataset()
        ds["latitude"] = 52
        ds["longitude"] = -120
        nearest = geo.get_nearest_station(
            ds["latitude"], ds["longitude"], stations=reference_stations
        )
        assert nearest, "Failed to return any stations"
        assert nearest == "first", "Failed to return the appropriate station"

    def test_nearest_station_with_reference_station_dataframe(self):
        df_reference_stations = pd.DataFrame(
            reference_stations, columns=["station", "latitude", "longitude"]
        )
        nearest = geo.get_nearest_station(52, -120, stations=df_reference_stations)
        assert nearest, "Failed to return any stations"
        assert nearest == "first", "Failed to return the appropriate station"


class GeoJSONTests(unittest.TestCase):
    def test_geojson_parser(self):
        collections = [geo.read_geojson(file) for file in geojson_files]
        assert collections
        assert isinstance(collections[0], dict)

    def test_geo_code(self):
        # parse test files
        geographical_areas = {}
        for file in geojson_files:
            geographical_areas.update(geo.read_geojson(file))

        lat, lon = 48.77228044489474, -62.36630494246806  # south of Anticosti Island
        geo_code = geo.get_geo_code((lon, lat), geographical_areas)
        assert isinstance(geo_code, str)
        assert geo_code == "Gulf-9"

    def test_geo_code_with_dataarray(self):
        # parse test files
        geographical_areas = {}
        for file in geojson_files:
            geographical_areas.update(geo.read_geojson(file))
        ds = xr.Dataset()
        ds["latitude"] = 48.77228044489474
        ds["longitude"] = -62.36630494246806
        geo_code = geo.get_geo_code(
            (ds["longitude"], ds["latitude"]), geographical_areas
        )
        assert isinstance(geo_code, str)
        assert geo_code == "Gulf-9"
