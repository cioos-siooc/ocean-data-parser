"""Set of tools used to parsed an NMEA string feed from a file."""

import logging

import numpy as np
import pandas as pd
import pynmea2

logger = logging.getLogger(__name__)

variable_mapping = {
    ("Heave", "heading"): (
        "Heave",
        "heave",
    )  # fix in https://github.com/Knio/pynmea2/pull/129 but not included in pipy yet
}

nmea_dtype_mapping = {
    "row": "Int64",
    "prefix": str,
    "talker": str,
    "sentence_type": str,
    "subtype": str,
    "manufacturer": str,
    "b_pressure_inch": float,
    "inches": str,
    "b_pressure_bar": float,
    "bars": str,
    "air_temp": float,
    "a_celsius": str,
    "water_temp": float,
    "w_celsius": str,
    "rel_humidity": float,
    "abs_humidity": float,
    "dew_point": float,
    "d_celsius": str,
    "direction_true": float,
    "true": str,
    "direction_magnetic": float,
    "magnetic": str,
    "wind_speed_knots": float,
    "knots": str,
    "wind_speed_meters": float,
    "meters": str,
    "datestamp": str,
    "timestamp": str,
    "status": str,
    "lat": str,
    "lat_dir": str,
    "lon": str,
    "lon_dir": str,
    "mag_variation": float,
    "mag_var_dir": str,
    "heading": float,
    "hdg_true": str,
    "wind_angle": float,
    "reference": str,
    "wind_speed": float,
    "wind_speed_units": str,
    "day": "Int64",
    "month": "Int64",
    "year": "Int64",
    "local_zone": str,
    "local_zone_minutes": "Int64",
    "gps_qual": "Int64",
    "num_sats": "Int64",
    "horizontal_dil": float,
    "altitude": float,
    "altitude_units": str,
    "geo_sep": float,
    "geo_sep_units": str,
    "age_gps_data": float,
    "ref_station_id": str,
    "true_track": float,
    "true_track_sym": str,
    "mag_track": float,
    "mag_track_sym": str,
    "spd_over_grnd_kts": float,
    "spd_over_grnd_kts_sym": str,
    "spd_over_grnd_kmph": float,
    "spd_over_grnd_kmph_sym": str,
    "faa_mode": str,
    "spd_over_grnd": float,
    "true_course": float,
    "_r": str,
    "true_heading": float,
    "is_true_heading": bool,
    "roll": float,
    "pitch": float,
    "roll_accuracy": str,
    "pitch_accuracy": str,
    "heading_accuracy": str,
    "aiding_status": str,
    "imu_status": str,
    "num_messages": "Int64",
    "msg_num": "Int64",
    "num_sv_in_view": float,
    "sv_prn_num_1": float,
    "elevation_deg_1": float,
    "azimuth_1": float,
    "snr_1": "Int64",
    "sv_prn_num_2": float,
    "elevation_deg_2": float,
    "azimuth_2": float,
    "snr_2": "Int64",
    "sv_prn_num_3": float,
    "elevation_deg_3": float,
    "azimuth_3": float,
    "snr_3": "Int64",
    "sv_prn_num_4": float,
    "elevation_deg_4": float,
    "azimuth_4": float,
    "snr_4": "Int64",
    "deviation": float,
    "dev_dir": str,
    "variation": float,
    "var_dir": str,
    "gps_datetime": None,
    "nmea_type": str,
    "latitude_degrees_north": float,
    "longitude_degrees_east": float,
    "wind_speed_relative_to_platform_knots": float,
    "wind_direction_relative_to_platform": float,
}


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
        return (-1 if self.lat_dir == "S" else 1) * (
            float(self.lat[:2]) + float(self.lat[2:]) / 60
        )


def _get_longitude(self):
    """Generate longitude in degree north from GGA/RMC/GLL information"""
    if self.get("lon"):
        return (-1 if self.lon_dir == "W" else 1) * (
            float(self.lon[:3]) + float(self.lon[3:]) / 60
        )


def _generate_extra_terms(nmea):
    """Generate extra terms from NMEA information
    Output is a dictionary with the keys following the convention:
    Args: pynmea2 object
    Return {
        (long_name, short_name):value,
        ...
    }
    """
    extra = {}
    if nmea["sentence_type"] in ("GGA", "RMC", "GLL"):
        extra.update(
            {
                ("Latitude", "latitude_degrees_north"): (
                    -1 if nmea["lat_dir"] == "S" else 1
                )
                * (float(nmea["lat"][:2]) + float(nmea["lat"][2:]) / 60),
                ("Longitude", "longitude_degrees_east"): (
                    -1 if nmea["lon_dir"] == "W" else 1
                )
                * (float(nmea["lon"][:3]) + float(nmea["lon"][3:]) / 60),
            }
        )
    if nmea["sentence_type"] == "ZDA":
        extra[("GPS Time", "gps_datetime")] = pd.to_datetime(
            f"{nmea['year']}-{nmea['month']}-{nmea['day']}T{nmea['timestamp']}",
            format=f"%Y-%m-%dT%H%M%S{'.%f' if len(nmea['timestamp'])>6 else''}",
            utc=True,
        )
    if nmea["sentence_type"] == "RMC":
        extra[("GPS Time", "gps_datetime")] = pd.to_datetime(
            f"{nmea['datestamp']}T{nmea['timestamp']}",
            format=f"%d%m%yT%H%M%S{'.%f' if len(nmea['timestamp'])>6 else''}",
            utc=True,
        )

    if nmea["sentence_type"] == "MWV" and nmea["reference"] == "R":
        if nmea["wind_speed_units"] == "N":
            units = ("knots", "knots")
        elif nmea["wind_speed_units"] == "M":
            units = ("m/s", "m_s")
        elif nmea["wind_speed_units"] == "K":
            units = ("km/h", "km_h")
        else:
            logger.error("unknown units for MWV: %s", nmea["wind_speed_units"])
        extra.update(
            {
                (
                    f"Wind Speed Relative To Platform [{units[0]}]",
                    f"wind_speed_relative_to_platform_{units[1]}",
                ): nmea["wind_speed"],
                (
                    "Wind Direction Relative To Platform",
                    "wind_direction_relative_to_platform",
                ): nmea["wind_angle"],
            }
        )
    return extra


def file(path, encoding="UTF-8", nmea_delimiter="$"):
    """Parse a file containing NMEA information into a pandas dataframe"""

    def rename_variable(name):
        """Rename variable based on variable mapping dictionary or return name"""
        return variable_mapping[name] if name in variable_mapping else name

    nmea = []
    long_names = {}
    with open(path, encoding=encoding) as f:

        for row, line in enumerate(f):
            try:
                prefix, nmea_string = line.split(nmea_delimiter)
                parsed_line = pynmea2.parse(nmea_delimiter + nmea_string)

                # Retrieve long_names from nmea fields
                long_names.update({field[1]: field[0] for field in parsed_line.fields})
                parsed_items = parsed_line.__dict__
                parsed_dict = {
                    "row": row,
                    "prefix": prefix,
                    "talker": parsed_items.get("talker"),
                    "sentence_type": parsed_items.get("sentence_type"),
                    "subtype": parsed_items.get("subtype"),
                    "manufacturer": parsed_items.get("manufacturer"),
                    **{
                        rename_variable(field[1]): value
                        for field, value in zip(parsed_line.fields, parsed_line.data)
                    },
                }
                # Get extra fields
                extra = _generate_extra_terms(parsed_dict)
                if extra:
                    long_names.update(
                        {short_name: long_names for long_names, short_name in extra}
                    )
                    # add extra fields
                    parsed_dict.update(
                        {short_name: value for (_, short_name), value in extra.items()}
                    )
                nmea += [parsed_dict]
            except pynmea2.ParseError:
                logger.error("Unable to parse line: %s", line, exc_info=True)
            except AttributeError:
                logger.error("Failed to retrieve atribue", exc_info=True)

    # Convert NMEA to a dataframe
    df = pd.DataFrame(nmea).replace({np.nan: None, "": None})

    # Cast variables to the appropriate type
    for col in df:
        if col not in nmea_dtype_mapping:
            logger.warning(
                "nmea column '%s' do not have a correspinding data type", col
            )
            continue
        if nmea_dtype_mapping[col] is None:
            continue
        try:
            df[col] = df[col].astype(nmea_dtype_mapping[col])
        except ValueError:
            logger.error("Failed to convert %s to %s", col, nmea_dtype_mapping[col])
    df = df.replace({"None": None})

    # Convert to xarray
    ds = df.to_xarray()

    # Add attributes
    # TODO Apply vocabulary
    for var in ds:
        if var in long_names:
            ds[var].attrs["long_name"] = long_names[var]
    return ds


def _generate_gps_variables(df):
    """Generate standardized variables from the different variables available"""
    # Generate variables
    df["nmea_type"] = df["talker"] + df["sentence_type"].fillna("manufacturer")
    df["gps_time"] = df.apply(_get_gps_time, axis=1)
    df["latitude_degrees_north"] = df.apply(_get_latitude, axis=1)
    df["longitude_degrees_east"] = df.apply(_get_longitude, axis=1)
    return df
