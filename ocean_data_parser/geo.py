import json
import os
from geographiclib.geodesic import Geodesic

from shapely.geometry import shape, Point


def read_geojson(
    filename,
    encoding="UTF-8",
):
    if not os.path.exists(filename):
        return None

    with open(filename, "r", encoding=encoding) as f:
        geojson = json.load(f)

    # Add shapely geometry
    geojson["features"] = [
        {**feature, **{"shape": shape(feature["geometry"]).buffer(0)}}
        for feature in geojson["features"]
    ]
    return geojson


def get_geo_code(position, collections):
    def _get_features_contains_position(features):
        return [
            feature
            for feature in features
            if feature["shape"].contains(Point(position))
        ]

    matched_features = []
    for collection in collections:
        matched_features += _get_features_contains_position(collection["features"])

    if matched_features:
        return ", ".join(
            [feature["properties"]["name"] for feature in matched_features]
        )


def get_nearest_station(
    latitude, longigude, stations, max_meter_distance_from_station=None, geod=None
):

    if geod is None:
        geod = Geodesic.WGS84  # define the WGS84 ellipsoid

    station_distance = {
        station: geod.Inverse(latitude, longigude, slat, slon)["s12"]
        for station, slat, slon in stations
    }

    nearest_station = min(station_distance, key=station_distance.get)
    distance_from_nearest_station = station_distance[nearest_station]
    if distance_from_nearest_station > max_meter_distance_from_station:
        return None
    return nearest_station
