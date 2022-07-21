"""Set of tools used to parsed an NMEA string feed from a file."""

import logging
from datetime import datetime

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
    "heading_true": float,
    "heading_magnetic": float,
    "" "hdg_true": str,
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
    "gps_datetime": datetime,
    "nmea_type": str,
    "latitude_degrees_north": float,
    "longitude_degrees_east": float,
    "wind_speed_relative_to_platform_knots": float,
    "wind_direction_relative_to_platform": float,
    "deg_r": float,
    "l_r": str,
    "wind_speed_kn": float,
    "unit_knots": str,
    "wind_speed_ms": float,
    "unit_ms": str,
    "wind_speed_km": float,
    "unit_km": str,
    "mode_indicator": str,
    "hdop": str,
    "diferential": str,
    "water_speed_knots": float,
    "water_speed_km": float,
    "kilometers": str,
    "source": str,
    "engine_no": str,
    "speed": float,
}


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
        extra[("GPS Time", "gps_datetime")] = datetime.strptime(
            f"{nmea['year']}-{nmea['month']}-{nmea['day']}T{nmea['timestamp']} UTC",
            f"%Y-%m-%dT%H%M%S{'.%f' if len(nmea['timestamp'])>6 else''} %Z",
        )
    if nmea["sentence_type"] == "RMC":
        extra[("GPS Time", "gps_datetime")] = datetime.strptime(
            f"{nmea['datestamp']}T{nmea['timestamp']} UTC",
            f"%d%m%yT%H%M%S{'.%f' if len(nmea['timestamp'])>6 else''} %Z",
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
            units = None
        if units:
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
            if not line:
                continue
            elif nmea_delimiter and nmea_delimiter not in line:
                logger.warning(
                    "Missing NMEA deliminter %s - ignore line %s",
                    nmea_delimiter,
                    line[:-1],
                )
                continue
            try:
                prefix, nmea_string = line.split(nmea_delimiter, 1)
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
            except (pynmea2.ParseError, AttributeError, ValueError, KeyError):
                logger.error("Unable to parse line: %s", line[:-1])

    # Convert NMEA to a dataframe
    df = pd.DataFrame(nmea).replace({np.nan: None, "": None})

    # Cast variables to the appropriate type
    for col in df:
        if col not in nmea_dtype_mapping:
            logger.warning(
                "nmea column '%s' do not have a corresponding data type", col
            )
    # Convert datetime columns
    datetime_cols = [
        var
        for var, dtype in nmea_dtype_mapping.items()
        if var in df and dtype in [datetime]
    ]
    for col in datetime_cols:
        df[col] = (
            pd.to_datetime(df[col], utc=True).dt.tz_convert(None).dt.to_pydatetime()
        )

    try:
        df = df.astype(
            {
                var: dtype
                for var, dtype in nmea_dtype_mapping.items()
                if var in df and var not in datetime_cols
            }
        )
    except ValueError:
        # Go one column at the time
        for var in [
            col
            for col in df.columns
            if col in nmea_dtype_mapping
            and nmea_dtype_mapping[col]
            and col not in datetime_cols
        ]:
            try:
                df[var] = df[var].astype(nmea_dtype_mapping[var])
            except ValueError:
                if df[var].dtype == object and nmea_dtype_mapping[var] == float:
                    df[var] = df[var].str.extract(r"(\d+\.*\d*)").astype(float)
                elif df[var].dtype == object and nmea_dtype_mapping[var] == int:
                    df[var] = df[var].str.extract(r"(\d+)").astype(float)
                else:
                    logger.error(
                        "Failed to convert NMEA variable %s to %s",
                        var,
                        nmea_dtype_mapping[var],
                        exc_info=True,
                    )

    df = df.replace({"None": None})

    # Convert to xarray
    ds = df.to_xarray()

    # Add attributes
    # TODO Apply vocabulary
    for var in ds:
        if var in long_names:
            ds[var].attrs["long_name"] = long_names[var]
    return ds
