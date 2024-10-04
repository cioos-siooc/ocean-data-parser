"""Python class to read IOS data files and store data for conversion to netcdf format.

Changelog Version
    0.1: July 15 2019:
        Convert python scripts and functions into a python class
    0.2: August 2023:
        Migrate the code to the ocean-data-parser package and reduce code base
Authors:
    Pramod Thupaki (pramod.thupaki@hakai.org)
    Jessy Barrette.

"""

import json
import logging
import re
import struct
from datetime import datetime, timedelta
from io import StringIO

import fortranformat as ff
import gsw
import numpy as np
import pandas as pd
import xarray as xr
from pytz import timezone

from ocean_data_parser import __version__
from ocean_data_parser.vocabularies.load import dfo_ios_vocabulary

logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(logger, {"file": None})

VERSION = __version__
DFO_IOS_SHELL_VOCABULARY = dfo_ios_vocabulary()
vocabulary_attributes = [
    "ios_name",
    "long_name",
    "standard_name",
    "units",
    "scale",
    "sdn_parameter_urn",
    "sdn_parameter_name",
    "sdn_uom_urn",
    "sdn_uom_name",
    "rename",
    "apply_func",
]

ios_dtypes_to_python = {
    "R": "float32",
    "F": "float32",
    "I": "int32",
    "D": str,
    "T": str,
    "C": str,
    "E": "float32",
}

global_attributes = {
    "institution": "DFO IOS",
    "ices_edmo_code": 4155,
    "sdn_institution_urn": "SDN:EDMO::4155",
    "infoUrl": (
        "https://science.gc.ca/site/science/en/educational-resources"
        "/marine-and-freshwater-sciences/institute-ocean-sciences"
    ),
    "country": "Canada",
    "ioc_country_code": 18,
    "naming_authority": "ca.gc.ios",
    "iso_3166_country_code": "CA",
    "platform_vocabulary": "https://vocab.nerc.ac.uk/search_nvs/C17/",
    "instrument_vocabulary": "https://vocab.nerc.ac.uk/search_nvs/L22/",
    "Conventions": "CF-1.6,CF-1.7,CF-1.8,ACDD1.1,ACDD-1.3,IOOS-1.2",
    "standard_name_vocabulary": "CF Standard Name Table v78",
    "creator_name": "Institute of Ocean Sciences (IOS)",
    "creator_institution": "Institute of Ocean Sciences (IOS)",
    "creator_email": "info@dfo-mpo.gc.ca",
    "creator_country": "Canada",
    "creator_sector": "gov_federal",
    "creator_url": "info@dfo-mpo.gc.ca",
    "creator_type": "institution",
}


def get_dtype_from_ios_type(ios_type):
    if not ios_type or ios_type.strip() == "":
        return
    elif ios_type in ios_dtypes_to_python:
        return ios_dtypes_to_python[ios_type]
    elif ios_type[0].upper() in ios_dtypes_to_python:
        return ios_dtypes_to_python[ios_type[0]]


def get_dtype_from_ios_name(ios_name):
    if re.search("flag", ios_name, re.IGNORECASE):
        return "int32"
    elif re.search("time|date", ios_name, re.IGNORECASE):
        return str
    else:
        return float


IOS_SHELL_HEADER_SECTIONS = {
    "FILE",
    "LOCATION",
    "COMMENTS",
    "REMARK",
    "ADMINISTRATION",
    "INSTRUMENT",
    "HISTORY",
    "DEPLOYMENT",
    "RECOVERY",
    "CALIBRATION",
}


class IosFile:
    """IOS File format parser.

    Class template for all the different data file types
    Contains data from the IOS file and methods to read the IOS format
    Specific improvements/modifications required
    to read filetypes will be make in derived classes
    Author: Pramod Thupaki pramod.thupaki@hakai.org
    Incorporates functions from earlier versions of this toolbox.
    """

    def __init__(self, filename):
        # initializes object by reading *FILE and ios_header_version
        # reads entire file to memory for all subsequent processing
        # inputs are filename and debug state

        logger.extra["file"] = filename
        self.type = filename.split(".", 1)[1]
        self.filename = filename
        self.start_date = None
        self.start_dateobj = None
        self.location = None
        self.channels = None
        self.comments = None
        self.remarks = None
        self.channel_details = None
        self.administration = None
        self.instrument = None
        self.data = None
        self.deployment = None
        self.recovery = None
        self.calibration = None
        self.obs_time = None
        self.vocabulary_attributes = None
        self.history = None

        # Load file
        try:
            with open(self.filename, encoding="ASCII") as file:
                self.lines = file.readlines()
        except UnicodeDecodeError:
            logger.warning("Bad characters were encountered. We will ignore them")
            with open(self.filename, encoding="ASCII", errors="ignore") as file:
                self.lines = file.readlines()

        self.ios_header_version = self.get_header_version()
        self.date_created = self.get_date_created()
        self.file = self.get_section("FILE")
        self.status = 1

    def import_data(self):
        sections_available = set(self.get_list_of_sections())
        self.start_dateobj, self.start_date = self.get_date(opt="start")
        self.end_dateobj, self.end_date = (
            self.get_date(opt="end") if "END TIME" in self.file else (None, None)
        )
        self.time_increment = self.get_dt() if "TIME INCREMENT" in self.file else None
        self.location = self.get_location()
        self.channels = self.get_channels()
        self.comments = self.get_comments_like("COMMENTS")
        self.remarks = self.get_comments_like("REMARKS")
        self.administration = self.get_section("ADMINISTRATION")
        self.instrument = self.get_section("INSTRUMENT")
        self.channel_details = self.get_channel_detail()
        self.history = self.get_section("HISTORY")
        if "DEPLOYMENT" in sections_available:
            self.deployment = self.get_section("DEPLOYMENT")
        if "RECOVERY" in sections_available:
            self.recovery = self.get_section("RECOVERY")
        if "CALIBRATION" in sections_available:
            self.calibration = self.get_section("CALIBRATION")

        unparsed_sections = sections_available - IOS_SHELL_HEADER_SECTIONS
        if unparsed_sections:
            logger.warning(
                "Unknown sections: %s",
                unparsed_sections,
            )

        if self.channel_details is None:
            logger.info("Unable to get channel details from header...")

        # try reading file using format specified in 'FORMAT' if failed ignore 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file.get("FORMAT"))
        except Exception as e:
            if not self.file.get("FORMAT"):
                raise e
            logger.info(
                "Failed to read data using FORMAT=%s ",
                self.file.get("FORMAT"),
            )
            self.data = self.get_data(formatline=None)

        # time variable
        self.rename_date_time_variables()
        chn_list = [
            channel_name.strip().lower() for channel_name in self.channels["Name"]
        ]
        if "date" in chn_list and ("time" in chn_list or "time:utc" in chn_list):
            self.get_obs_time_from_date_time()
        elif self.get_file_extension().lower() in ("cur", "loop", "drf"):
            self.get_obs_time_from_time_increment()

    def get_date_created(self):
        return pd.to_datetime(self.lines[0][1:], utc=True)

    def get_header_version(self):
        # reads header version
        return self.lines[self.find_index("*IOS HEADER VERSION")][20:24]

    def find_index(self, string):
        # finds line number that starts with string
        # input: string (nominally the section)
        for index, line in enumerate(self.lines):
            if line.lstrip()[0 : len(string)] == string:
                return index

        logger.debug("Index not found %s", string)
        return -1

    def get_complete_header(self):
        # return all sections in header as a dict
        sections = self.get_list_of_sections()
        header = {}
        for sec in sections:
            # logger.info ("getting section:", sec)
            if sec in ["COMMENTS", "REMARKS", "HISTORY"]:
                header[sec] = self.get_comments_like(sec)
            else:
                header[sec] = self.get_section(sec)
        return header

    def get_section(self, section_name: str) -> dict:
        # deciphers the information in a particular section
        # reads table information
        # returns information as dictionary.
        # records (subsections) are returned as list of lines for subsequent processing
        def _get_subsection(idx) -> list:
            subsection = []
            while self.lines[idx + 1].strip()[0:4] != "$END":
                idx += 1
                subsection += [self.lines[idx]]
            return subsection, idx + 1

        if section_name[0] != "*":
            section_name = "*" + section_name
        idx = self.find_index(section_name)
        if idx == -1:
            logger.info("Section not found" + section_name + self.filename)
            return {}
        info = {}
        # EOS = False # end of section logical
        while True:
            idx += 1
            line = self.lines[idx]
            if not line.strip() or line[0] == "!":
                # skip blank or commented lines
                continue
            elif line[0] in ["$", "*"]:
                break
            elif "$" in line[1:5]:
                # read record or 'sub-section'.
                # This nominally starts with tab of 4 spaces
                # but can be 1 or 2 spaces as well for REMARKS
                subsection_name = line.strip()
                logger.debug(
                    "Found subsection:%s in section:%s", subsection_name, section_name
                )
                info[subsection_name], idx = _get_subsection(idx)
            else:
                logger.debug(line)
                if ":" in line:
                    key, value = line.split(":", 1)
                    info[key.strip()] = value
        return info

    def get_flag_convention(self, name: str, units: str = None) -> dict:
        if name.lower() == "flag:at_sea":
            return {
                "rename": "flag:at_sea",
                "flag_values": [0, 1, 2, 3, 4, 5],
                "flag_meanings": " ".join(
                    [
                        "not_classified",
                        "good:at_sea_freely_floating",
                        "bad:at_sea_but_trapped_in_rocky_intertidal",
                        "bad:on_land",
                        "bad:at_sea bad:land_travel",
                    ]
                ),
                "units": None,
            }
        elif units.lower() == "igoss_flags":
            return {
                "flag_values": [0, 1, 2, 3, 4, 5],
                "flag_meanings": " ".join(
                    [
                        "not_checked",
                        "appears_to_be_good",
                        "inconsistent_with_climatology",
                        "appears_to_be_doubtful",
                        "appears_to_be_wrong",
                        "value_was_changed_see_history_record",
                    ]
                ),
            }
        elif name.lower() == "flag:ctd" or name.lower() == "flag":
            return {
                "flag_values": [0, 2, 6],
                "flag_meanings": " ".join(
                    [
                        "not_quality_control",
                        "good",
                        "interpolated_or_replaced_by_dual_sensor_or_upcast_value",
                    ]
                ),
            }
        elif name.lower().startswith("flag") and self.filename.endswith("che"):
            return {
                "flag_values": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                "flag_meanings": " ".join(
                    [
                        "sample_drawn_from_water_bottle_but_not_analyzed",
                        "acceptable_measurement",
                        "questionable_measurement",
                        "bad_measurement",
                        "not_reported",
                        "mean_of_replicate_measurement",
                        "manual_chromatographic_peak_measurement",
                        "irregular_digital_chromatographic_peak_integration",
                        "sample_not_drawn_for_this_measurement_from_this_bottle",
                    ]
                ),
            }

        elif name.lower() == "sample_method":
            return {
                "flag_values": ["UN", "US", "USM"],
                "flag_meanings": " ".join(["no_stop", "stop_for_30s", "up_stop_mix"]),
            }
        logger.warning("Unknown flag name=%s, units=%s", name, units)
        return {}

    def get_file_extension(self):
        if "." in self.filename:
            return self.filename.rsplit(".", 1)[1]
        return None

    def get_subsection(self, name):
        # return subsection information from a section
        # used as interface for data actually read into dictionary by get_section
        # provides some useful debug information
        # returns lines that make up 'subsection' if all is well
        if name not in self.file.keys():
            logger.info("Unvailable subsection:%s", name)
            return None
        return self.file[name]

    def get_dt(self):
        # converts time increment from ios format to seconds
        # float32 accurate (seconds are not rounded to integers)
        if "TIME INCREMENT" not in self.file or "n/a" in self.file["TIME INCREMENT"]:
            logger.warning("Time Increment not found in Section:FILE")
            return

        line = self.file["TIME INCREMENT"].split("!")[0].split()
        dt = np.asarray(line, dtype=float)
        return sum(dt * [24.0 * 3600.0, 3600.0, 60.0, 1.0, 0.001])  # in seconds

    def get_date(self, opt="start"):
        # reads datetime string in "START TIME" and converts to datetime object
        # return datetime object and as standard string format
        # read 'END TIME' if opt is 'end'
        if "START TIME" not in self.file:
            raise Exception("START TIME: not available in file", self.filename)

        if opt.lower() == "start":
            date_string = self.file["START TIME"].strip().upper()
        elif opt.lower() == "end":
            date_string = self.file["END TIME"].strip().upper()
        else:
            raise Exception("Invalid option for get_date function !")

        if "!" in date_string:
            date_string, warn_msg = date_string.split("!", 1)
            logger.warning("Date string has warning: %s", warn_msg)

        logger.debug("Raw date string: %s", date_string)
        # get the naive (timezone unaware) datetime obj
        try:
            date_obj = datetime.strptime(date_string[4:], "%Y/%m/%d %H:%M:%S.%f")
        except ValueError:
            if re.match(r"\d{4}\/\d{2}\/\d{2}", date_string[:4]):
                logger.warning("No time available is available will assume midnight.")
                date_obj = datetime.strptime(date_string[4:], "%Y/%m/%d")
            else:
                logger.warning(
                    "Use pandas pd.to_datetime to parse date string: %s",
                    date_string[4:],
                )
                date_obj = pd.to_datetime(date_string[4:])
            logger.info(date_obj)
        # make datetime object, aware of its timezone
        # for GMT, UTC
        if any([date_string.find(z) == 0 for z in ["GMT", "UTC"]]):
            date_obj = timezone(date_string[0:3]).localize(date_obj)
        # for PST/PDT
        elif "PST" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=8))
        elif "PDT" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=7))
        # Canada/Mountain
        elif "MST" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=7))
        elif "MDT" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=6))
        # Canada/Atlantic
        elif "AST" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=4))
        elif "ADT" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=3))
        else:
            logger.warning(
                "Problem finding the timezone from->'%s' will default to UTC ",
                date_string,
            )
            date_obj = timezone("UTC").localize(date_obj)

        logger.debug("Date obj with timezone info: %s", date_obj)
        # convert all datetime to utc before writing to netcdf file
        date_obj = date_obj.astimezone(timezone("UTC"))
        return date_obj, date_obj.strftime("%Y/%m/%d %H:%M:%S.%f %Z")

    def fmt_len(self, fmt):
        # deprecated: calculated length of string from 'struct' format specification
        # assumes on 's' data fromat
        return np.asarray(fmt[0:-1].split("s"), dtype="int").sum()

    def add_to_history(self, input):
        if not hasattr(self, "history"):
            self.history = {}
        if "ios_transform_history" not in self.history:
            self.history["ios_transform_history"] = "IOS Transform History:\n"
        self.history["ios_transform_history"] += (
            f"{datetime.now().isoformat()} - {input}\n"
        )

    def get_data(self, formatline=None):
        # reads data using the information in FORMAT
        # if FORMAT information in file header is missing or does not work
        # then create 'struct' data format based on channel details information
        idx = self.find_index("*END OF HEADER")
        lines = self.lines[idx + 1 :]
        data = []
        # if formatline is None, try reading without any format
        # (assume columns are space limited;
        #   if space limited strategy does not work, try to create format line)
        if formatline is None:
            try:
                logger.debug(
                    "Trying to read file using format created using column width"
                )
                logger.debug(
                    "Reading data using format %s", self.channel_details["fmt_struct"]
                )
                fmt_len = self.fmt_len(self.channel_details["fmt_struct"])
                fmt_struct = self.channel_details["fmt_struct"]
                for i in range(len(lines)):
                    if len(lines[i].strip()) > 1:
                        data.append(
                            struct.unpack(
                                fmt_struct,
                                lines[i].rstrip().ljust(fmt_len).encode("utf-8"),
                            )
                        )
            except Exception:
                data = np.genfromtxt(
                    StringIO("".join(lines)), delimiter="", dtype=str, comments=None
                )
                logger.info("Reading data using delimiter was successful !")

        else:
            ffline = ff.FortranRecordReader(formatline)
            for i in range(len(lines)):
                if len(lines[i]) > 0 and not re.match("\x1a+", lines[i]):
                    data.append([float(r) for r in ffline.read(lines[i])])
        data = np.asarray(data)
        logger.debug(data)
        # if data is at only one, convert list to 2D matrix
        if len(data.shape) == 1:
            data = data.reshape((1, -1))
        return data

    def get_location(self):
        # read 'LOCATION' section from ios header
        # convert lat and lon to standard format (float, -180 to +180)
        # initialize some other standard section variables if possible
        def _convert_latlong_string(ll):
            if not isinstance(ll, str):
                return ll
            # ignore trailing comments
            ll = ll.rsplit("!")[0]
            buf = ll.split()
            direction = -1 if len(buf) == 3 and buf[2] in ("S", "W") else 1
            return direction * (float(buf[0]) + float(buf[1]) / 60)

        info = self.get_section("LOCATION")
        logger.debug("Location details %s", info.keys())
        # Convert lat and lon
        info["LATITUDE"] = _convert_latlong_string(info.get("LATITUDE"))
        info["LONGITUDE"] = _convert_latlong_string(info.get("LONGITUDE"))
        return info

    def get_channel_detail(self):
        # read channel details. create format_structure (fmt_struct)
        # based on channel details. This information may be used as backup if
        # file does not contain FORMAT specifier
        # tpp: modified to read all variables as strings.
        # This is done because 'Format' information in 'CHANNEL DETAIL'
        # is not a fortran compatible description
        # CHANGELOG July 2019: decipher python 'struct' format from channel details
        lines = self.get_subsection("$TABLE: CHANNEL DETAIL")
        if lines is None:
            return None
        mask = lines[1].rstrip()
        ch_det = [self.apply_col_mask(line, mask) for line in lines[2:]]
        info = {
            "Pad": [line[1] for line in ch_det],
            "Width": [line[3] for line in ch_det],
            "Format": [line[4] for line in ch_det],
            "Type": [line[5] for line in ch_det],
        }
        if int(self.file["NUMBER OF CHANNELS"]) != len(info["Pad"]):
            raise Exception(
                "Number of channels in file record does not match channel_details!"
            )
        elif not any([item for item in info["Type"] if item.strip()]) or not any(
            [item for item in info["Format"] if item.strip()]
        ):
            return info

        fmt = ""
        for i in range(len(info["Pad"])):
            if info["Type"][i].strip() == "D":
                fmt += "11s"
            elif info["Type"][i].strip() == "DT":
                fmt += "17s"
            elif info["Format"][i].strip().upper() == "HH:MM:SS.SS":
                fmt += "12s"
            elif info["Format"][i].strip().upper() == "HH:MM:SS":
                fmt += "9s"
            elif info["Format"][i].strip().upper() == "HH:MM":
                fmt += "6s"
            elif info["Width"][i].strip():
                fmt += info["Width"][i].strip() + "s"
            elif re.match(r"F\d+\.\d+", info["Format"][i], re.IGNORECASE):
                fmt += (
                    re.match(r"F(\d+)\.\d+\s*", info["Format"][i], re.IGNORECASE)[1]
                    + "s"
                )
            elif re.match(r"I\d+", info["Format"][i], re.IGNORECASE):
                fmt += re.match(r"I(\d+)", info["Format"][i], re.IGNORECASE)[1] + "s"
            elif info["Format"][i].strip() in ("F", "I", "f", "i"):
                logger.info(
                    "Unable to retrieve the fmt format from " "the CHANNEL DETAIL Table"
                )
                break
            else:
                raise Exception(
                    f"Unknown variable format Format: {info['Format'][i]}, Type: {info['Type'][i]}"
                )
        else:
            info["fmt_struct"] = fmt

        logger.debug("Python compatible data format: %s", fmt)
        return info

    def get_channels(self):
        # get the details of al the channels in the file
        # return as dictionary with each column as list
        lines = self.get_subsection("$TABLE: CHANNELS")
        mask = lines[1].rstrip()
        channels = [self.apply_col_mask(line, mask) for line in lines[2:]]
        return {
            "Name": [line[1] for line in channels],
            "Units": [line[2] for line in channels],
            "Minimum": [line[3] for line in channels],
            "Maximum": [line[4] for line in channels],
        }

    def apply_col_mask(self, data, mask):
        # apply mask to string (data) to get columns
        # return list of columns
        logger.debug("data=%s, mask=%s", data, mask)
        data = data.rstrip().ljust(len(mask))
        a = [d == "-" for d in mask]
        ret = []
        quoted = False
        pass_column_limit = False
        for i in range(len(data)):
            # Some IOS tables have quoted values that extend over
            # the limits of the colmns
            if data[i] == "'":
                if quoted and pass_column_limit:
                    pass_column_limit = False
                    ret.append("*")
                quoted = not quoted
            elif not a[i] and not quoted:
                ret.append("*")
            elif not a[i]:
                pass_column_limit = True
            else:
                ret.append(data[i])
        buf = "".join(ret).split("*")
        while "" in buf:
            buf.remove("")
        return buf

    def get_comments_like(self, section_name):
        # to read sections like comments/remarks etc that are at 'root' level
        # and contain a lot of information that must be kept together
        # return information as a dictionary with identifier being line number
        if section_name[0] != "*":
            section_name = "*" + section_name.strip()
        idx = self.find_index(section_name)
        if idx == -1:
            return ""
        info = {}
        # EOS = False # end of section logical
        count = 0
        while True:
            idx += 1
            count += 1
            line = self.lines[idx]
            if len(line.strip()) == 0:  # skip line if blank
                continue
            elif line[0] == "!":
                continue
            elif line[0] in ["$", "*"]:
                break
            else:
                logger.debug(line)
                info[f"{count:d}"] = line.rstrip()
        return info

    def get_list_of_sections(self):
        # parse the entire header and returns list of sections available
        # skip first 2 lines of file (that has date and ios_header_version)
        # skip * in beginning of section name
        sections_list = [
            line.strip()[1:]
            for line in self.lines[2:]
            if (
                line[0] == "*"
                and line[0:4] != "*END"
                and line[1] not in ["*", " ", "\n"]
            )
        ]
        logger.debug(sections_list)
        return sections_list

    def assign_geo_code(self, polygons_dict, unknown_geographical_area="None"):
        # TODO use the ocean-data-parser equivalent
        def find_geographic_area(poly_dict, point):
            return "".join(
                [key for key in poly_dict if poly_dict[key].contains(point)]
            ).replace(" ", "-")

        try:
            from shapely.geometry import Point
        except ImportError:
            logger.error("Missing package shapely, please install shapely")

        self.geo_code = (
            find_geographic_area(
                polygons_dict,
                Point(self.location["LONGITUDE"], self.location["LATITUDE"]),
            )
            or unknown_geographical_area
        )

    def get_obs_time_from_date_time(self):
        # Return a timeseries
        chn_list = [i.strip().lower() for i in self.channels["Name"]]

        if "time:utc" in chn_list:
            chn_list[chn_list.index("time:utc")] = "time"

        if "date" in chn_list and "time" in chn_list:
            if isinstance(self.data[0, chn_list.index("date")], bytes):
                dates = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chn_list.index("date")]
                ]
                times = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chn_list.index("time")]
                ]
            else:
                dates = [i.strip() for i in self.data[:, chn_list.index("date")]]
                times = [i.strip() for i in self.data[:, chn_list.index("time")]]
            datetime = pd.to_datetime(
                [date.replace(" ", "") + " " + time for date, time in zip(dates, times)]
            )
            self.obs_time = datetime
            self.obs_time = [
                timezone("UTC").localize(i + timedelta(hours=0)) for i in self.obs_time
            ]
        elif "date" in chn_list:
            if isinstance(self.data[0, chn_list.index("date")], bytes):
                dates = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chn_list.index("date")]
                ]
            else:
                dates = [i.strip() for i in self.data[:, chn_list.index("date")]]
            datetime = pd.to_datetime(dates)
            self.obs_time = datetime
            self.obs_time = [
                timezone("UTC").localize(i + timedelta(hours=0)) for i in self.obs_time
            ]
        else:
            logger.error("Unable to find date/time columns in variables")
            return 0
        # Test result
        self.compare_obs_time_to_star_date()

    def get_obs_time_from_time_increment(self):
        time_increment = self.get_dt()
        self.obs_time = [
            self.start_dateobj + timedelta(seconds=time_increment * (i))
            for i in range(int(self.file["NUMBER OF RECORDS"]))
        ]
        # Test result
        self.compare_obs_time_to_star_date()

    def compare_obs_time_to_star_date(self, dt=pd.Timedelta("1minute")):
        if not (-dt < self.obs_time[0] - self.start_dateobj < dt):
            logger.error(
                "First record does not match start date: obs_time[0]-start_dateobj=%s",
                self.obs_time[0] - self.start_dateobj,
            )
            return 0

    def add_ios_vocabulary(self):
        def match_term(reference, value):
            if reference in (None, np.nan):
                return False
            if (
                ("None" in reference.split("|") and value in (None, "n/a", ""))
                or re.fullmatch(reference, value)
                or value in reference.split("|")
            ):
                return True
            return False

        # Filter vocabulary to handle only file extension and global terms
        vocab = (
            DFO_IOS_SHELL_VOCABULARY.query(
                f"ios_file_extension == '{self.get_file_extension().lower()}' or "
                "ios_file_extension.isna()"
            )
            .sort_values("ios_file_extension")
            .set_index("ios_file_extension")
        )
        vocab.index = vocab.index.fillna("all")

        # iterate over variables and find matching vocabulary
        self.vocabulary_attributes = []
        for name, units in zip(self.channels["Name"], self.channels["Units"]):
            # Drop trailing spaces and commas
            name = re.sub(r"^\'|[\s\']+$", "", name.lower())
            units = re.sub(r"^\'|[\s\']+$", "", units)

            if re.match(r"\'*(flag|quality_flag|sample_method$)", name, re.IGNORECASE):
                self.vocabulary_attributes += [[self.get_flag_convention(name, units)]]
                continue
            if re.match("(Date|Time)", name, re.IGNORECASE):
                self.vocabulary_attributes += [[{}]]
                continue

            units = re.sub(r"^'|'$", "", units)
            name_match_type = vocab["ios_name"] == name.strip().lower()
            match_units = vocab["accepted_units"].apply(
                lambda x: match_term(x, units.strip())
            )

            matched_vocab = vocab.loc[name_match_type & match_units]
            if matched_vocab.empty:
                logger.warning(
                    "Missing vocabulary for file_type=%s; variable name=%s,units=%s",
                    self.filename.rsplit(".", 1)[1],
                    name,
                    units,
                )
                self.vocabulary_attributes += [[{"long_name": name, "units": units}]]
                continue

            # Consider only the vocabularies specific to this ios_file_extension group
            matched_vocab = matched_vocab.query(
                f'ios_file_extension == "{matched_vocab.index.get_level_values(0)[0]}"'
            )
            self.vocabulary_attributes += [
                [
                    row.dropna().to_dict()
                    for _, row in matched_vocab[vocabulary_attributes].iterrows()
                ]
            ]

    def fix_variable_names(self):
        # get variable name list
        variables = [item.strip() for item in self.channels["Name"]]

        bad_character_index = [
            index for index, name in enumerate(self.channels["Name"]) if "%" in name
        ]
        if bad_character_index:
            for index in bad_character_index:
                self.channels["Name"][index] = self.channels["Name"][index].replace(
                    "%", "perc"
                )

        # Rename duplicated Quality_Flag:Phos flags in UBC files
        if (
            variables.count("Quality_Flag:Phos") == 2
            and "Phosphate(inorg)" in variables
            and "Phosphate" in variables
        ):
            ids = [
                id for id, item in enumerate(variables) if "Quality_Flag:Phos" in item
            ]
            if variables.index("Phosphate(inorg)") != ids[1] - 1:
                logger.error(
                    "Phosphate(inorg) isn't preceding the Quality_Flag:Phos variable."
                )

            logger.info(
                "Rename duplicated flag 'Quality_Flag:Phos' -> 'Quality_Flag:Phosphate(inorg)'"
            )
            self.channels["Name"][ids[-1]] = "Quality_Flag:Phosphate(inorg)"

        if any(variable.endswith("[ml/l]") for variable in variables):
            logger.warning("Units [ml/l] present within variable name will be dropped")
            for id, variable in enumerate(variables):
                if variable.endswith("[ml/l]"):
                    self.channels["Name"][id] = variable[:-7].strip()

    def rename_date_time_variables(self):
        rename_channels = self.channels["Name"]
        history = []
        for id, (chan, units) in enumerate(
            zip(self.channels["Name"], self.channels["Units"])
        ):
            if (
                chan.startswith("Time")
                and units.strip().lower() in ("days", "day_of_year")
            ) or chan.strip().lower() in ["time:day_of_year", "time:julian"]:
                rename_channels[id] = "Time:Day_of_Year"
            elif not re.search("^(time|date)", chan, re.IGNORECASE) or chan.strip() in (
                "Time",
                "Date",
            ):
                continue
            elif re.match(r"Date[\s\t]*($|YYYY/MM{1,2}/D{1,2})", chan.strip()):
                logger.warning("Rename variable '%s' -> 'Date'", chan)
                rename_channels[id] = "Date"
            elif re.match(r"Time[\s\t]*($|HH:MM:SS)", chan.strip()):
                logger.warning("Rename variable '%s' -> 'Time'", chan)
                history += [f"rename variable '{chan}' -> 'Time'"]
                rename_channels[id] = "Time"
            else:
                logger.warning("Unkown date time channel %s", chan)

        self.channels["Name"] = rename_channels

    def get_global_attributes(self):
        def _format_attributes(section, prefix=""):
            def _format_attribute_name(name):
                if name == "$REMARKS":
                    return f"{section}_remarks"
                return (
                    f"{prefix}{name}".replace(" ", "_")
                    .replace("(", "")
                    .replace(")", "")
                    .lower()
                )

            def _format_attribute_value(value):
                if isinstance(value, str):
                    return value.replace("! custom item", "").strip()
                elif isinstance(value, (float, int)):
                    return value
                elif isinstance(value, list):
                    return "".join(value)
                else:
                    return value

            attrs = getattr(self, section)
            if attrs:
                return {
                    _format_attribute_name(name): _format_attribute_value(value)
                    for name, value in attrs.items()
                    if value and not name.startswith("$TABLE:")
                }
            else:
                return {}

        # Generate global attributes
        return {
            "id": self.filename,
            **_format_attributes("administration"),
            **{
                key: value
                for key, value in _format_attributes("file").items()
                if key not in ["format", "data_type", "file_type"]
            },
            **_format_attributes("instrument", prefix="instrument_"),
            **_format_attributes("location"),
            **_format_attributes("deployment", "deployment_"),
            **_format_attributes("recovery", "recovery_"),
            "calibration": json.dumps(self.calibration) if self.calibration else None,
            "comments": str(self.comments)
            if self.comments
            else None,  # TODO missing file_remarks
            "remarks": str(self.remarks) if self.remarks else None,
            "history": str(self.history) if hasattr(self, "history") else None,
            "geographic_area": self.geo_code if hasattr(self, "geo_code") else None,
            "headers": json.dumps(
                self.get_complete_header(), ensure_ascii=False, indent=False
            ),
            "start_time": self.start_dateobj.isoformat(),
            "end_time": self.end_dateobj.isoformat() if self.end_dateobj else None,
            "source": self.filename,
            "ios_header_version": self.ios_header_version,
            "ocean_data_transform_version": VERSION,
            "product_version": f"ios_header={self.ios_header_version}; ocean-data-transform={VERSION}",
            "date_created": self.date_created.isoformat(),
            **global_attributes,
        }

    def to_xarray(
        self,
        rename_variables=True,
        append_sub_variables=True,
        replace_date_time_variables=True,
    ):
        """Convert ios class to xarray dataset.

        Returns:
            xarray dataset
        """

        def update_variable_index(varname, id):
            """Update variable index (1,01,X,XX) by the given index or append."""
            if varname.endswith(("01", "XX")):
                return f"{varname[:-2]}{id:02g}"
            elif varname.endswith(("1", "X")):
                return f"{varname[:-1]}{id}"
            return f"{varname}{id:02g}"

        def _drop_empty_attrs(attrs):
            if isinstance(attrs, dict):
                return {key: value for key, value in attrs.items() if value}
            return attrs

        def _flag_bad_values(dataset):
            bad_values = [-9.99, -99.9, -99.0, -99.999, -9.9, -999.0, -9.0]
            var_with_bad_values = [
                var
                for var, values in (dataset.isin(bad_values)).any().items()
                if values.item(0)
            ]
            if not var_with_bad_values:
                return dataset

            for var in var_with_bad_values:
                bad_values = list(
                    filter(
                        lambda x: x == x,
                        np.unique(
                            dataset[var].where(dataset[var].isin(bad_values)).values
                        ),
                    )
                )
                logger.warning(
                    "Suspicious values = %s were detected and will replaced by NaN",
                    bad_values,
                )

            return dataset.where(~dataset.isin(bad_values))

        # Retrieve the different variable attributes
        variables = (
            pd.DataFrame(
                {
                    "ios_name": self.channels["Name"],
                    "units": self.channels["Units"],
                    "ios_type": self.channel_details.get("Type")
                    if self.channel_details
                    else "",
                    "ios_format": self.channel_details.get("Format")
                    if self.channel_details
                    else "",
                    "pad": self.channel_details.get("Pad")
                    if self.channel_details
                    else "",
                }
            )
            .map(str.strip)
            .replace({"": None, "n/a": None})
        )
        variables["matching_vocabularies"] = self.vocabulary_attributes
        variables["dtype"] = (
            variables["ios_type"]
            .fillna(variables["ios_format"])
            .apply(get_dtype_from_ios_type)
            .fillna(variables["ios_name"].apply(get_dtype_from_ios_name))
        )

        variables["_FillValues"] = variables.apply(
            lambda x: pd.Series(x["pad"]).astype(x["dtype"]).values[0]
            if x["pad"]
            else None,
            axis="columns",
        )
        variables["renamed_name"] = variables.apply(
            lambda x: x["matching_vocabularies"][-1].get("rename", x["ios_name"]),
            axis="columns",
        )

        # Detect duplicated variables ios_name,units pairs
        duplicates = variables.duplicated(subset=["ios_name", "units"], keep=False)
        if duplicates.any():
            logger.warning(
                "Duplicated variables (Name,Units) pair detected, "
                "only the first one will be considered:\n%s",
                variables.loc[duplicates][["ios_name", "units"]],
            )
            variables.drop_duplicates(
                subset=["ios_name", "units"], keep="first", inplace=True
            )

        # Detect and rename duplicated variable names with different units
        col_name = "ios_name"
        duplicated_name = variables.duplicated(subset=[col_name])
        if duplicated_name.any():
            variables["var_index"] = variables.groupby(col_name).cumcount()
            to_replace = duplicated_name & (variables["var_index"] > 0)
            new_names = variables.loc[to_replace].apply(
                lambda x: update_variable_index(x[col_name], x["var_index"] + 1),
                axis="columns",
            )
            logger.info(
                "Duplicated variable names, will rename the variables according to: %s",
                list(
                    zip(
                        variables.loc[
                            to_replace, list(set(["ios_name", "units"] + [col_name]))
                        ]
                        .reset_index()
                        .values.tolist(),
                        "renamed -> " + new_names,
                    )
                ),
            )
            variables.loc[to_replace, col_name] = new_names

        # Parse data, assign appropriate data type, padding values
        #  and convert to xarray object
        ds = (
            pd.DataFrame.from_records(
                self.data[:, variables.index], columns=variables[col_name]
            )
            .replace(r"\.$", "", regex=True)
            .astype(dict(variables[[col_name, "dtype"]].values))
            .replace(
                dict(variables[[col_name, "_FillValues"]].dropna().values), value=np.nan
            )
            .to_xarray()
        )
        ds = _flag_bad_values(ds)
        ds.attrs = self.get_global_attributes()

        # Add variable attributes
        if append_sub_variables is True:
            ds_sub = xr.Dataset()
            ds_sub.attrs = ds.attrs

        for id, row in variables.iterrows():
            var = ds[row[col_name]]
            var.attrs = _drop_empty_attrs(
                {
                    "original_ios_variable": str(
                        {id: row[["ios_name", "units"]].to_json()}
                    ),
                    "original_ios_name": row["ios_name"],
                    "long_name": row["ios_name"],
                    "units": row["units"],
                }
            )
            if not append_sub_variables:
                var.attrs["sub_variables"] = json.dumps(row["matching_vocabularies"])
                continue
            elif not row["matching_vocabularies"]:
                ds_sub.assign({var.name: var})
                continue

            # Generate vocabulary variables
            for new_var_attrs in row["matching_vocabularies"]:
                new_var = new_var_attrs.pop("rename", row[col_name])

                # if variable already exist from a different source variable
                #  append variable index
                if new_var in ds_sub:
                    if (
                        ds_sub[new_var].attrs["original_ios_name"]
                        == var.attrs["original_ios_name"]
                    ):
                        logger.error(
                            "Duplicated vocabulary output for %s, will be ignored", row
                        )
                        continue
                    else:
                        new_index = (
                            len([var for var in ds_sub if var.startswith(new_var[:-1])])
                            + 1
                        )
                        logger.warning(
                            "Duplicated variable from sub variables: %s, renamed %s",
                            new_var,
                            update_variable_index(new_var, new_index),
                        )
                        new_var = update_variable_index(new_var, new_index)

                if "apply_func" in new_var_attrs:
                    ufunc = eval(new_var_attrs["apply_func"], {"ds": ds, "gsw": gsw})
                    new_data = xr.apply_ufunc(ufunc, var)
                    self.add_to_history(
                        f"Generate new variable from {row[col_name]} ->"
                        f" apply {new_var_attrs['apply_func']}) -> {new_var}"
                    )

                else:
                    new_data = var
                    self.add_to_history(
                        f"Generate new variable from {row[col_name]} -> {new_var}"
                    )

                ds_sub[new_var] = (
                    var.dims,
                    new_data.data,
                    _drop_empty_attrs({**var.attrs, **new_var_attrs}),
                )

        if append_sub_variables:
            ds = ds_sub

        # coordinates
        if self.obs_time and replace_date_time_variables:
            ds = ds.drop_vars([var for var in ds if var in ["Date", "Time"]])
            ds["time"] = (ds.dims, pd.Series(self.obs_time))
            # ds["time"].encoding["units"] = "seconds since 1970-01-01T00:00:00Z"
        elif self.start_dateobj:
            ds["time"] = self.start_dateobj

        ds.attrs["time_coverage_resolution"] = (
            pd.Timedelta(self.time_increment).isoformat()
            if self.time_increment
            else None
        )

        if "latitude" not in ds and "latitude" in ds.attrs:
            ds["latitude"] = ds.attrs["latitude"]
            ds["latitude"].attrs = {
                "long_name": "Latitude",
                "units": "degrees_north",
                "standard_name": "latitude",
            }
            ds["longitude"] = ds.attrs["longitude"]
            ds["longitude"].attrs = {
                "long_name": "Longitude",
                "units": "degrees_east",
                "standard_name": "longitude",
            }

        # Define dimensions
        if "time" in ds and ds["time"].dims and ds["index"].size == ds["time"].size:
            ds = ds.swap_dims({"index": "time"})
        elif "depth" in ds and ds["index"].size == ds["depth"].size:
            ds = ds.swap_dims({"index": "depth"})

        # Define featureType attribute
        if "time" in ds.dims:
            if (
                "latitude" in ds
                and "time" in ds["latitude"].dims
                and "longitude" in ds
                and "time" in ds["longitude"].dims
            ):
                feature_type = "trajectory"
            else:
                feature_type = "timeSeries"
        else:
            feature_type = ""
        if "depth" in ds.dims:
            feature_type += "Profile" if feature_type else "profile"
        ds.attrs["featureType"] = feature_type
        ds.attrs["cdm_data_type"] = feature_type.title()

        # Set coordinate variables
        coordinates_variables = ["time", "latitude", "longitude", "depth"]
        if any(var in ds for var in coordinates_variables):
            ds = ds.set_coords([var for var in coordinates_variables if var in ds])
            if "index" in ds.coords and ("time" in ds.coords or "depth" in ds.coords):
                ds = ds.reset_coords("index").drop("index")

        # Drop empty attributes and variable attribtes
        ds.attrs = {key: value for key, value in ds.attrs.items() if value}
        for var in ds:
            ds[var].attrs = {
                key: value for key, value in ds[var].attrs.items() if value
            }
        return ds
