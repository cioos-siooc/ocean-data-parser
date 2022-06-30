"""Set of tools used to parsed an NMEA string feed from a file."""

import logging

import numpy as np
import pandas as pd
import pynmea2

logger = logging.getLogger(__name__)


def _get_gps_time(self):
    """Generate pandas Timestamp object from VTG and GGA NMEA information"""
    if self.get("timestamp") is None:
        return pd.NaT

    # Review time format
    time_format = "%H%M%S" if len(self["timestamp"]) == 6 else "%H%M%S.%f"
    # Convert to timestamp
    if self.get("datestamp"):

        return pd.to_datetime(
            f"{self['datestamp']}T{self['timestamp']}",
            format=f"%d%m%yT{time_format}",
            utc=True,
        )
    elif self.get("year") and self.get("month") and self.get("day"):
        return pd.to_datetime(
            f"{self['year']}-{self['month']}-{self['day']} {self['timestamp']}",
            format=f"%Y-%m-%d {time_format}",
            utc=True,
        )


def _get_latitude(self):
    """Generate latitude in degree north from GGA/RMC/GLL information"""
    if self.get("lat"):
        return (-1 if "lat_dir" == "S" else 1) * (
            float(self["lat"][:2]) + float(self["lat"][3:]) / 60
        )


def _get_longitude(self):
    """Generate longitude in degree north from GGA/RMC/GLL information"""
    if self.get("lon"):
        return (-1 if "lon_dir" == "W" else 1) * (
            float(self["lon"][:3]) + float(self["lon"][4:]) / 60
        )

def _generate_gps_variables(df):
    """Generate standardized variables from the different variables available"""
    # Generate variables
    df["nmea_type"] = df["talker"] + df["sentence_type"].fillna("manufacturer")
    df["gps_time"] = df.apply(_get_gps_time, axis=1)
    df["latitude_degrees_north"] = df.apply(_get_latitude, axis=1)
    df["longitude_degrees_east"] = df.apply(_get_longitude, axis=1)
    return df

def file(path, encoding="UTF-8", nmea_delimiter="$"):
    """Parse a file containing NMEA information into a pandas dataframe"""
    nmea = []
    with open(path, encoding=encoding) as f:

        for row, line in enumerate(f):
            try:
                prefix, nmea_string = line.split(nmea_delimiter)
                parsed_line = pynmea2.parse(nmea_delimiter + nmea_string)
                parsed_items = parsed_line.__dict__
                nmea += [
                    {
                        "row": row,
                        "prefix": prefix,
                        "talker": parsed_items.get("talker"),
                        "sentence_type": parsed_items.get("sentence_type"),
                        "subtype": parsed_items.get("subtype"),
                        "manufacturer": parsed_items.get("manufacturer"),
                        **{
                            field[1]: value
                            for field, value in zip(
                                parsed_line.fields, parsed_line.data
                            )
                        },
                    }
                ]

            except pynmea2.ParseError:
                logger.error("Unable to parse line: %s", line, exc_info=True)
            except AttributeError:
                logger.error("Failed to retrieve atribue", exc_info=True)

    # Convert NMEA to a dataframe
    df = pd.DataFrame(nmea).astype(object).replace(np.nan, None)
    df = _generate_gps_variables(df)

    # Convert to xarray
    ds = df.to_xarray()
    # TODO Apply vocabulary

    return ds


def _generate_gps_variables(df):
    """Generate standardized variables from the different variables available"""
    # Generate variables
    df["nmea_type"] = df["talker"] + df["sentence_type"].fillna("manufacturer")
    df["gps_time"] = df.apply(_get_gps_time, axis=1)
    df["latitude_degrees_north"] = df.apply(_get_latitude, axis=1)
    df["longitude_degrees_east"] = df.apply(_get_longitude, axis=1)
    return df
