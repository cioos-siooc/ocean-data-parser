import logging
import re

from ocean_data_parser.geo import get_nearest_station, get_geo_code
import ocean_data_parser.read.dfo.odf_source.attributes as attributes

logger = logging.getLogger(__name__)


def generate_geospatial_attributes(dataset, config):
    """Generate the geographic_area and station attirbutes based on the cdm_data_type and data coordinates"""
    # Define coordinates variables from attributes, assign geographic_area and nearest stations
    dataset = attributes.generate_coordinates_variables(dataset)
    if (
        dataset.attrs["cdm_data_type"] in ("Profile", "Timeseries")
        and "latitude" in dataset
        and "longitude" in dataset
    ):
        dataset.attrs["geographic_area"] = get_geo_code(
            [dataset["longitude"].mean(), dataset["latitude"].mean()],
            config["geographic_areas"],
        )

        nearest_station = get_nearest_station(
            dataset["latitude"],
            dataset["longitude"],
            config["reference_stations"][["station", "latitude", "longitude"]].values,
            config["maximum_distance_from_station_km"],
        )
        if nearest_station:
            dataset.attrs["station"] = nearest_station
        elif (
            dataset.attrs.get("station")
            and dataset.attrs.get("station")
            not in config["reference_stations"]["station"].tolist()
            and re.match(r"[^0-9]", dataset.attrs["station"])
        ):
            logger.warning(
                "Station %s [%sN, %sE] is missing from the reference_station.",
                dataset.attrs["station"],
                dataset["latitude"].mean().values,
                dataset["longitude"].mean().values,
            )
    return dataset
