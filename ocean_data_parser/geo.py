import json
import os
from geographiclib.geodesic import Geodesic

from shapely.geometry import shape, Point


def read_geojson(
    path: str,
    encoding: str = "UTF-8",
) -> dict:

    """Parse geojson files and return it as a dictionary.

    If features are available, generate a shapely feature object.

    Args:
        path: The path to the geojson file to read.
        encoding [UTF-8]: The encoding of the geojson file.
    Returns:
        parsed geojson dictionary (dict)
    """
    if not os.path.exists(path):
        return None

    with open(path, "r", encoding=encoding) as f:
        geojson = json.load(f)

    # Add shapely geometry
    if "features" in geojson:
        geojson["features"] = [
            {**feature, **{"shape": shape(feature["geometry"]).buffer(0)}}
            for feature in geojson["features"]
        ]
    return geojson


def get_geo_code(position: list, geographical_areas_collections: list) -> str:
    """get_geo_code generate for a given position (longitude, latitude)
    the list of associated geographical areas available
    within the collections.

    Args:
        position (float,float): [description]
        collections (list): [description]
    Returns:
        geographical_areas list (str): comma separated list of matching geographical areas
    """

    def _get_features_contains_position(features):
        return [
            feature
            for feature in features
            if feature["shape"].contains(Point(position))
        ]

    matched_features = []
    for collection in geographical_areas_collections:
        matched_features += _get_features_contains_position(collection["features"])

    if matched_features:
        return ", ".join(
            [feature["properties"]["name"] for feature in matched_features]
        )


def get_nearest_station(
    latitude: float,
    longigude: float,
    stations: list,
    max_meter_distance_from_station: float = None,
    geod: Geodesic = None,
) -> str:
    """AI is creating summary for get_nearest_station

    Args:
        latitude (float): [description]
        longigude (float): [description]
        stations (list): [description]
        max_meter_distance_from_station (float, optional): [description]. Defaults to None.
        geod (Geodesic, optional): [description]. Defaults to None.

    Returns:
        nearest_station (str): Nearest station to the given latitude and longitude
    """
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
