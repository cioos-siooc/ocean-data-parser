import json
import logging
import re
from datetime import datetime, timezone
from difflib import get_close_matches

import pandas as pd

from ocean_data_parser.parsers.seabird import (
    _get_seabird_instrument_from_header,
    _get_seabird_processing_history,
)
from ocean_data_parser.vocabularies.load import dfo_platforms

logger = logging.getLogger(__name__)

stationless_programs = ("Maritime Region Ecosystem Survey",)

# Transform platform name to a list of accepted platform names
reference_platforms = dfo_platforms()
reference_platforms["accepted_platform_name"] = reference_platforms[
    "accepted_platform_name"
].str.split("|")
reference_platforms = reference_platforms.explode("accepted_platform_name")

section_prefix = {
    "EVENT_HEADER": "event_",
    "INSTRUMENT_HEADER": "instrument_",
}

global_odf_to_cf = {
    "event_creation_date": "date_modified",
    "event_orig_creation_date": "date_issued",
    "start_date": "cruise_start_date",
    "end_date": "cruise_end_date",
}


def _generate_platform_attributes(platform: str) -> dict:
    """Review ODF CRUISE_HEADER:PLATFORM and match to closest."""
    if reference_platforms is None:
        return {}
    platform = re.sub(
        r"CCGS_*\s*|CGCB\s*|FRV\s*|NGCC\s*|^_|MV\s*", "", platform
    ).strip()
    matched_platform = get_close_matches(
        platform.lower(), reference_platforms["accepted_platform_name"], n=1
    )
    if matched_platform:
        return (
            reference_platforms.query(
                f"accepted_platform_name == '{matched_platform[0]}'"
            )
            .iloc[0]
            .to_dict()
        )

    logger.warning("Unknown platform %s", platform)
    return {}


def _generate_cf_history_from_odf(odf_header) -> dict:
    """Generate CF compatible history from ODF header.

    Follow the CF conventions for history attribute: iso timestamp - description.
    If a Seabird instrument csv header is provided, it will be converted to a CF standard and
    made available within the instrument_manufacturer_header attribute.
    Processing steps associated with the SBE Processing toolbox will also be
    incorporated within the history attribute.
    """

    def _add_to_history(comment, date=datetime.now(timezone.utc)):
        """Generate a CF standard history line."""
        date_str = (
            date.strftime("%Y-%m-%dT%H:%M:%SZ")
            if pd.notna(date)
            else "0000-00-00 00:00:00Z"
        )
        return f"{date_str} {comment}\n"

    # Convert ODF history to CF history
    is_manufacturer_header = False

    history = {
        "instrument_manufacturer_header": "",
        "internal_processing_notes": "",
        "history": "",
    }
    for history_group in odf_header["HISTORY_HEADER"]:
        if "PROCESS" not in history_group:
            continue
        # Convert single processes to list
        if isinstance(history_group["PROCESS"], str):
            history_group["PROCESS"] = [history_group["PROCESS"]]

        # Empty history group (just write the date)
        if history_group["PROCESS"] is None:
            history["history"] += _add_to_history("", history_group["CREATION_DATE"])
            continue

        for row in history_group["PROCESS"]:
            if row is None:
                continue
            # Retrieve Instrument Manufacturer Header
            if row.startswith("* Sea-Bird"):
                history["history"] += "# SEA-BIRD INSTRUMENTS HEADER\n"
                is_manufacturer_header = True
            if is_manufacturer_header:
                history["instrument_manufacturer_header"] += row + "\n"
            else:
                history["internal_processing_notes"] += _add_to_history(
                    row, history_group["CREATION_DATE"]
                )

            # End of manufacturer header
            if row.startswith("*END*"):
                is_manufacturer_header = False
                history["history"] += "# ODF Internal Processing Notes\n"

            # Ignore some specific lines within the history (mostly seabird header ones)
            if re.match(
                r"^(\#\s*\<.*|\*\* .*"
                + r"|\# (name|span|nquan|nvalues|unit|interval|start_time|bad_flag)"
                + r"|\* |\*END\*)",
                row,
            ):
                continue
            # Add to cf history
            history["history"] += _add_to_history(row, history_group["CREATION_DATE"])
    return history


def _define_cdm_data_type_from_odf(odf_header: dict) -> dict:
    """Generate cdm_data_type attributes based on the odf data_type attribute."""
    # Derive cdm_data_type from DATA_TYPE
    odf_data_type = odf_header["EVENT_HEADER"]["DATA_TYPE"]
    attributes = {"odf_data_type": odf_data_type}
    if odf_data_type in ["CTD", "BOTL", "BT", "XBT"]:
        attributes.update(
            {
                "cdm_data_type": "Profile",
                "cdm_profile_variables": "",
            }
        )

        if odf_data_type == "CTD":
            attributes["profile_direction"] = odf_header["EVENT_HEADER"][
                "EVENT_QUALIFIER2"
            ]
    elif odf_data_type in ["MCM", "MCTD", "MMOB", "MTC", "MTG", "MTR"]:
        attributes.update(
            {
                "cdm_data_type": "Timeseries",
                "cdm_timeseries_variables": "",
            }
        )
    elif odf_data_type in ["MADCP"]:
        attributes.update(
            {
                "cdm_data_type": "TimeseriesProfile",
                "cdm_timeseries_variables": "",
                "cdm_profile_variables": "",
            }
        )
    elif odf_data_type in ["TCTD", "TSG"]:
        attributes.update(
            {"cdm_data_type": "Trajectory", "cdm_trajectory_variables": ""}
        )
    elif odf_data_type == "PLNKG":
        attributes["cdm_data_type"] = "Point"
    else:
        logger.error(
            "ODF parser is not yet incompatible with ODF DATA_TYPE: %s",
            odf_data_type,
        )
    return attributes


def _review_event_number(global_attributes, odf_header) -> int:
    """Review event_number which should be number otherwise get rid of it."""
    # If interger already return that same value
    if isinstance(global_attributes["event_number"], int):
        return global_attributes["event_number"]
    elif isinstance(global_attributes["event_number"], str) and re.match(
        r"\d+", global_attributes["event_number"]
    ):
        return int(global_attributes["event_number"].replace("P", ""))

    # Look for an event_number withih all the original header
    event_number = re.search(
        r"\*\* Event[\s\:\#]*(\d+)",
        "".join(odf_header["original_header"]),
        re.IGNORECASE,
    )
    if event_number:
        return int(event_number[1])
    logger.warning(
        "event_number %s is not just a number",
        global_attributes["event_number"],
    )


def _standardize_station_names(station: str) -> str:
    """Standardize stations.

    Standardize station name with convention:
    - ABC01: capital letters two digits
    - 001: 3 digits numbers
    - Otherwise unchanged.
    """
    if re.match(r"[A-Za-z]+\_*\d+", station):
        station_items = re.search(r"([A-Za-z]+)_*(\d+)", station).groups()
        return f"{station_items[0].upper()}{int(station_items[1]):02g}"
    # Station is just number convert to string with 001
    elif re.match(r"^[0-9]+$", station):
        return f"{int(station):03g}"
    else:
        return station


def _review_station(global_attributes, odf_header):
    """Review station attribute.

    The station attribute is reviewed based on the following rules:
    - If not available search in original odf header for "station... : STATION_NAME"
    - Standardize station name
    - Make sure station != event_number.
    """
    # If station is already available return it back
    if global_attributes.get("station"):
        return global_attributes["station"]
    elif global_attributes.get("project", "") in stationless_programs:
        return None

    # Search station anywhere within ODF Header
    station = re.search(
        r"station[\w\s]*:\s*([A-Za-z]+_*\d+)|\s+(\d+)\s*;nom de la station",
        "".join(odf_header["original_header"]),
        re.IGNORECASE,
    )
    if not station:
        return

    # If station is found standardize it
    station = _standardize_station_names([item for item in station.groups() if item][0])

    # Ignore station that are actually the event_number
    if re.match(r"^[0-9]+$", station) and int(station) == global_attributes.get(
        "event_number"
    ):
        logger.warning(
            "Station name is suspicious since its just a number similar to the event_number: %s",
            station,
        )
    return station


def _generate_instrument_attributes(odf_header, instrument_manufacturer_header=None):
    """Generate instrument attributes.

    The generated attributes are  based on:
    - ODF instrument attribute
    - manufacturer header.
    """
    # Instrument Specific Information
    attributes = {}
    if instrument_manufacturer_header:
        attributes["instrument"] = _get_seabird_instrument_from_header(
            instrument_manufacturer_header
        )
        attributes["seabird_processing_modules"] = _get_seabird_processing_history(
            instrument_manufacturer_header
        )
    elif "INSTRUMENT_HEADER" in odf_header:
        attributes["instrument"] = " ".join(
            [
                odf_header["INSTRUMENT_HEADER"].get("INST_TYPE") or "",
                odf_header["INSTRUMENT_HEADER"].get("MODEL") or "",
            ]
        ).strip()
        attributes["instrument_serial_number"] = (
            odf_header["INSTRUMENT_HEADER"].get("SERIAL_NUMBER") or ""
        )
    else:
        logger.warning("No Instrument field available")
        attributes["instrument"] = ""
        attributes["instrument_serial_number"] = ""

    if re.search(
        r"(SBE\s*(9|16|19|25|37))|Sea-Bird|CTD|Guildline|GUILDLN|GUILDLIN|GULIDLIN|GLDLNE|GULDLNEDIG|GUIDLINE|GUIDELINE|DIGITAL|GLD3NO.2|STD|RCM",
        attributes["instrument"],
        re.IGNORECASE,
    ):
        attributes["instrument_type"] = "CTD"
    elif re.search(r"Bathythermograph Manual", attributes["instrument"]):
        attributes["instrument_type"] = "BT"
    else:
        logger.warning(
            "Unknown instrument type for instrument: %s; odf['INSTRUMENT_HEADER']: %s",
            attributes["instrument"],
            odf_header.get("INSTRUMENT_HEADER"),
        )
    return attributes


def _generate_title_from_global_attributes(attributes):
    org_inst = [
        item
        for item in [attributes.get("organization"), attributes.get("institution")]
        if item
    ]

    title = (
        f"{attributes['odf_data_type']} profile data collected "
        + (
            f"from the {attributes['platform']} {attributes.get('platform_name', '')}"
            if "platform" in attributes
            else ""
        )
        + (f" by {' '.join(org_inst) if org_inst else ''}")
        + f" on the {attributes['cruise_name'].title()} "
    )
    if (
        pd.notna(attributes["cruise_start_date"])
        and pd.notna(attributes["cruise_end_date"])
        and isinstance(attributes["cruise_start_date"], datetime)
        and isinstance(attributes["cruise_end_date"], datetime)
    ):
        title += (
            f"from {attributes['cruise_start_date'].strftime('%d-%b-%Y')} "
            + f"to {attributes['cruise_end_date'].strftime('%d-%b-%Y')}."
        )
    return title


def _generate_program_specific_attritutes(global_attributes):
    """Generate program specific attributes.

    Specific attributes are generated for the following programs:
    Bedford Institute of Oceanography
    - AZMP
        + Program specific -> cruise_name = None
    - MARES
    - AZOMP.
    """
    # Standardize project and cruise_name (AZMP, AZOMP and MARES)
    if "program" not in global_attributes:
        return {}

    program = global_attributes["program"]
    project = global_attributes.get("project")
    year = global_attributes["event_start_time"].year
    month = global_attributes["event_start_time"].month

    if program == "Atlantic Zone Monitoring Program":
        season = "Spring" if 1 <= month <= 7 else "Fall"
        return {
            "project": project or f"{program} {season}",
            "cruise_name": None if project else f"{program} {season} {year}",
        }

    elif program == "Maritimes Region Ecosystem Survey":
        season = "Summer" if 5 <= month <= 9 else "Winter"
        return {
            "project": f"{program} {season}",
            "cruise_name": f"{program} {season} {year}",
        }
    elif program in [
        "Atlantic Zone Off-Shelf Monitoring Program",
        "Barrow Strait Monitoring Program",
    ]:
        return {"cruise_name": f"{program} {year}"}
    else:
        return {}


def _map_odf_to_cf_globals(attrs):
    """Map ODF attributes to cf,acdd names."""
    return {global_odf_to_cf.get(name, name): value for name, value in attrs.items()}


def global_attributes_from_header(dataset, odf_header):
    """Retrieve global attributes from ODF Header and apply corrections.

    Method use to define the standard global attributes from an ODF Header
    parsed by the read function.
    """
    # Generate Global attributes
    dataset.attrs = {
        # CRUISE_HEADER
        **{
            f"cruise_{name.lower()}"
            if name in ("START_DATE", "END_DATE", "CHIEF_SCIENTIST")
            else name.lower(): value
            for name, value in odf_header["CRUISE_HEADER"].items()
        },
        # EVENT_HEADER
        **{
            name.lower() if name.startswith("EVENT") else f"event_{name}".lower(): value
            for name, value in odf_header["EVENT_HEADER"].items()
        },
        # INSTRUMENT_HEADER
        **{
            f"instrument_{name.replace('inst_', '')}".lower(): value
            for name, value in odf_header.get("INSTRUMENT_HEADER", {}).items()
        },
        **_generate_cf_history_from_odf(odf_header),
        "original_odf_header": "\n".join(odf_header["original_header"]),
        "original_odf_header_json": json.dumps(
            odf_header, ensure_ascii=False, indent=False, default=str
        ),
        **_generate_platform_attributes(odf_header["CRUISE_HEADER"]["PLATFORM"]),
        **_define_cdm_data_type_from_odf(odf_header),
        **dataset.attrs,
    }
    # Apply global attributes corrections
    dataset.attrs.update(
        {
            **_generate_instrument_attributes(
                odf_header, dataset.attrs.get("instrument_manufacturer_header")
            ),
            "title": _generate_title_from_global_attributes(dataset.attrs),
            "cruise_chief_scientist": _standardize_chief_scientist(
                dataset.attrs["cruise_chief_scientist"]
            ),
            "event_number": _review_event_number(dataset.attrs, odf_header),
            "station": _review_station(dataset.attrs, odf_header),
            **_generate_program_specific_attritutes(dataset.attrs),
        }
    )
    dataset.attrs = _map_odf_to_cf_globals(dataset.attrs)

    # Review ATTRIBUTES
    if isinstance(dataset.attrs.get("comments"), list):
        dataset.attrs["comments"] = "\n".join(
            [line for line in dataset.attrs["comments"] if line]
        )

    # Ignore empty attributes
    dataset.attrs = {
        key: value
        for key, value in dataset.attrs.items()
        if value not in (None, pd.NaT)
    }
    return dataset


def generate_coordinates_variables(dataset):
    """Method use to generate metadata variables from the ODF Header to a xarray Dataset."""
    if "cdm_data_type" not in dataset.attrs:
        logging.error("No cdm_data_type attribute")

    if dataset.attrs["cdm_data_type"] in ("Profile", "Point"):
        dataset["time"] = (
            [],
            dataset.attrs["event_start_date_time"],
            {
                "name": "time",
                "standard_name": "time",
                "ioos_category": "Time",
                "coverage_content_type": "coordinate",
                "timezone": "UTC",
            },
        )

    if dataset.attrs["cdm_data_type"] in (
        "Profile",
        "Point",
        "Timeseries",
        "TimeseriesProfile",
    ):
        dataset["latitude"] = (
            [],
            dataset.attrs["event_initial_latitude"],
            {
                "long_name": "Latitude",
                "units": "degrees_north",
                "standard_name": "latitude",
                "ioos_category": "Location",
                "coverage_content_type": "coordinate",
            },
        )
        dataset["longitude"] = (
            [],
            dataset.attrs["event_initial_longitude"],
            {
                "long_name": "Longitude",
                "units": "degrees_east",
                "standard_name": "longitude",
                "ioos_category": "Location",
                "coverage_content_type": "coordinate",
            },
        )

    return dataset


def _standardize_chief_scientist(name):
    """Apply minor corrections to chief_scientist.

    The following corrections are applied:
    - replace separator ~, / by ,
    - Ignore Dr.
    """
    name = re.sub(r"\s+(\~|\/)", ",", name)
    return re.sub(r"(^|\s)(d|D)r\.{0,1}", "", name).strip().title()
