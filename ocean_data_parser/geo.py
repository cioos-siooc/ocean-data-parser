import json
import os
from typing import Union

import pandas as pd


def read_geojson(
    path: str,
    encoding: str = "UTF-8",
) -> dict:
    """Parse geojson files and return it as a dictionary.

    If features are available, generate a shapely feature object.

    Args:
        path: The path to the geojson file to read.
        encoding: The file encoding. Defaults to "UTF-8".

    Returns:
        parsed geojson dictionary (dict)
    """
    try:
        from shapely.geometry import shape
    except ImportError:
        raise RuntimeError(
            "Shapely is necessary to read geojson. "
            "Install shapely with `pip install shapely`"
        )

    if not os.path.exists(path):
        return None

    with open(path, encoding=encoding) as f:
        geojson = json.load(f)

    # Add shapely geometry
    for feature in geojson["features"]:
        if feature["geometry"]["type"] == "Polygon":
            geojson[feature["properties"]["name"]] = shape(feature["geometry"]).buffer(
                0
            )
    return geojson


def get_geo_code(position: list, geographical_areas_collections: list) -> str:
    """Get geocode for a given position (longitude, latitude).

    The list of associated geographical areas available
    within the collections.

    Args:
        position (float,float): [description]
        geographical_areas_collections (list): collecton of geographical areas and their
            associated polygons.

    Returns:
        geographical_areas list (str): comma separated list of
            matching geographical areas
    """
    try:
        from shapely.geometry import Point, Polygon
    except ImportError:
        raise RuntimeError(
            "Shapely is necessary to retrieve geograpical areas. "
            "Install shapely with `pip install shapely`"
        )
    matched_features = [
        name.replace(" ", "-")
        for name, polygon in geographical_areas_collections.items()
        if isinstance(polygon, Polygon) and polygon.contains(Point(position))
    ]

    return " ".join(matched_features) if matched_features else "n/a"


def get_nearest_station(
    latitude: float,
    longitude: float,
    stations: Union[list[tuple[str, float, float]], pd.DataFrame],
    max_distance_from_station_km: float = None,
    geod: str = "WGS84",
) -> str:
    """Get the nearest station from a list of reference stations.

    Args:
        latitude (float): target latitude: target latitude.
        longitude (float): target longitude: target longitude.
        stations  (list, pd.DataFrame): List of reference stations
            [(station, latitude, longitude)] or pandas DataFrame
            if a dataframe is passed, the expected colums should be
            respectively called (station, latitude,longitude)
        max_distance_from_station_km (float, optional): Max distance in
            kilometer from station to be matched.
        geod (Geodesic, optional): geographicLib Geodesic model. Defaults to WGS84.

    Returns:
        nearest_station (str): Nearest station to the given latitude and longitude
    """
    try:
        from geographiclib.geodesic import Geodesic

        geod = getattr(Geodesic, geod)  # define the WGS84 ellipsoid
    except ImportError:
        raise RuntimeError(
            "geographiclib is necessary to run get_nearest_station. "
            "Install geographiclib with `pip install geographicLib`"
        )

    if isinstance(stations, pd.DataFrame):
        stations = stations[["station", "latitude", "longitude"]].values

    station_distance = {
        station: geod.Inverse(latitude, longitude, slat, slon)["s12"]
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
