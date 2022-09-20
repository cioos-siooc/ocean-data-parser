import json
import os
from geographiclib.geodesic import Geodesic

from shapely.geometry import shape, Point, Polygon


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
    for feature in geojson["features"]:
        if feature["geometry"]["type"] == "Polygon":
            geojson[feature["properties"]["name"]] = shape(feature["geometry"]).buffer(
                0
            )
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

    matched_features = [
        name.replace(" ", "-")
        for name, polygon in geographical_areas_collections.items()
        if isinstance(polygon, Polygon) and polygon.contains(Point(position))
    ]

    return " ".join(matched_features) if matched_features else "n/a"


def get_nearest_station(
    latitude: float,
    longigude: float,
    stations: list,
    max_distance_from_station_km: float = None,
    geod: Geodesic = None,
) -> str:
    """AI is creating summary for get_nearest_station

    Args:
        latitude (float): [description]
        longigude (float): [description]
        stations (list): [description]
        max_distance_from_station_km (float, optional): Max distance [km] from station to be matched.
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
    if (
        max_distance_from_station_km
        and distance_from_nearest_station / 1000 > max_distance_from_station_km
    ):
        return None
    return nearest_station
