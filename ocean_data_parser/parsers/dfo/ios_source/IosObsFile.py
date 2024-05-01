"""
Python class to read IOS data files and store data for conversion to netcdf format
Changelog Version
    0.1: July 15 2019:
        Convert python scripts and functions into a python class
    0.2: August 2023:
        Migrate the code to the ocean-data-parser package and reduce code base
Authors:
    Pramod Thupaki (pramod.thupaki@hakai.org)
    Jessy Barrette

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

from ocean_data_parser._version import __version__
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

IOS_TYPE_MAPPING = {
    "R": "float32",
    "F": "float32",
    "I": "int32",
    "D": str,
    "T": str,
    "C": str,
    "E": "float32",
}

GLOBAL_ATTRIBUTES = {
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


def _cast_ios_variable(ios_type, ios_format, ios_name):
    dtype = (ios_type or ios_format or "").strip().upper()
    if dtype and dtype[0] in IOS_TYPE_MAPPING:
        return IOS_TYPE_MAPPING[dtype[0]]
    elif re.search("flag", ios_name, re.IGNORECASE):
        return "int32"
    elif re.search("time|date", ios_name, re.IGNORECASE):
        return str
    logger.info(
        "Unknown data type for variable %s [Type=%s, Format=%s], default to 'float32'",
        ios_name,
        ios_type,
        ios_format,
    )
    return "float32"


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


class IosFile(object):
    """
    Class template for all the different data file types
    Contains data from the IOS file and methods to read the IOS format
    Specific improvements/modifications required
    to read filetypes will be make in derived classes
    Author: Pramod Thupaki pramod.thupaki@hakai.org
    Incorporates functions from earlier versions of this toolbox
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
        self.geo_code = None

        # Load file
        try:
            with open(self.filename, "r", encoding="ASCII") as file:
                self.lines = file.readlines()
        except UnicodeDecodeError:
            logger.info("Bad characters were encountered. We will ignore them")
            with open(self.filename, "r", encoding="ASCII", errors="ignore") as file:
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
        chnList = [
            channel_name.strip().lower() for channel_name in self.channels["Name"]
        ]
        if "date" in chnList and ("time" in chnList or "time:utc" in chnList):
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

        common_attrs = {
            "ios_name": name.lower(),
            "rename": name.lower(),
            "standard_name": "quality_flag",
        }
        
        if name.lower() == "flag:at_sea":
            return {
                **common_attrs,
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
                **common_attrs,
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
                **common_attrs,
                "flag_values": [0, 2, 6],
                "flag_meanings": " ".join(
                    [
                        "not_quality_control",
                        "good",
                        "interpolated_or_replaced_by_dual_sensor_or_upcast_value",
                    ]
                ),
            }
        elif name.lower().startswith("flag") and self.filename.endswith(("che",'bot')):
            return {
                **common_attrs,
                "flag_values": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                "flag_meanings": " ".join(
                    [   
                        "acceptable_measurement_with_no_header_comment",
                        "sample_drawn_from_water_bottle_but_not_analyzed_sample_lost",
                        "acceptable_measurement_with_header_comment",
                        "questionable_measurement(probably_bad)",
                        "poor_measurement(probably_bad)",
                        "not_reported(bad)",
                        "mean_of_replicate_measurement",
                        "manual_chromatographic_peak_measurement",
                        "irregular_digital_chromatographic_peak_integration",
                        "sample_not_drawn_for_this_measurement_from_this_bottle",
                    ]
                ),
            }

        elif name.lower() == "sample_method":
            return {
                **common_attrs,
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
        self.history[
            "ios_transform_history"
        ] += f"{datetime.now().isoformat()} - {input}\n"

    def get_data(self, formatline=None) -> list:
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

    def get_location(self) -> dict:
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

    def get_channel_detail(self) -> dict:
        # read channel details. create format_structure (fmt_struct)
        # based on channel details. This information may be used as backup if
        # file does not contain FORMAT specifier
        # tpp: modified to read all variables as strings.
        # This is done because 'Format' information in 'CHANNEL DETAIL'
        # is not a fortran compatible description
        # CHANGELOG July 2019: decipher python 'struct' format from channel details
        lines = self.get_subsection("$TABLE: CHANNEL DETAIL")
        if lines is None:
            return {}
        mask = lines[1].rstrip()
        ch_det = [self.apply_col_mask(line, mask) for line in lines[2:]]
        info = {
            "Pad": [line[1].strip() for line in ch_det],
            "Width": [line[3].strip() for line in ch_det],
            "Format": [line[4].strip() for line in ch_det],
            "Type": [line[5].strip() for line in ch_det],
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
                    "Unknown variable format Format: %s, Type: %s"
                    % (info["Format"][i], info["Type"][i])
                )
        else:
            info["fmt_struct"] = fmt

        logger.debug("Python compatible data format: %s", fmt)
        return info

    def get_channels(self) -> dict:
        # get the details of al the channels in the file
        # return as dictionary with each column as list
        lines = self.get_subsection("$TABLE: CHANNELS")
        mask = lines[1].rstrip()
        channels = [self.apply_col_mask(line, mask) for line in lines[2:]]
        return {
            "Name": [line[1].strip() for line in channels],
            "Units": [line[2].strip() for line in channels],
            "Minimum": [line[3].strip() for line in channels],
            "Maximum": [line[4].strip() for line in channels],
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
                info["{:d}".format(count)] = line.rstrip()
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
        chnList = [i.strip().lower() for i in self.channels["Name"]]

        if "time:utc" in chnList:
            chnList[chnList.index("time:utc")] = "time"

        if "date" in chnList and "time" in chnList:
            if isinstance(self.data[0, chnList.index("date")], bytes):
                dates = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chnList.index("date")]
                ]
                times = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chnList.index("time")]
                ]
            else:
                dates = [i.strip() for i in self.data[:, chnList.index("date")]]
                times = [i.strip() for i in self.data[:, chnList.index("time")]]
            datetime = pd.to_datetime(
                [date.replace(" ", "") + " " + time for date, time in zip(dates, times)]
            )
            self.obs_time = datetime
            self.obs_time = [
                timezone("UTC").localize(i + timedelta(hours=0)) for i in self.obs_time
            ]
        elif "date" in chnList:
            if isinstance(self.data[0, chnList.index("date")], bytes):
                dates = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chnList.index("date")]
                ]
            else:
                dates = [i.strip() for i in self.data[:, chnList.index("date")]]
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
            # Drop trailing spaces and quotes
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
                self.vocabulary_attributes += [[{"long_name": name, "units": units,"ios_name": name}]]
                continue

            # Consider only the vocabularies specific to this ios_file_extension group
            matched_vocab = matched_vocab.query(
                f'ios_file_extension == "{matched_vocab.index.get_level_values(0)[0]}"'
            )
            self.vocabulary_attributes += [matched_vocab[vocabulary_attributes].to_dict('records')]

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
            "comments": self.comments,
            "remarks": self.remarks,
            "history": self.history,
            # "geographic_area": self.geo_code,
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
            **GLOBAL_ATTRIBUTES,
        }

    def to_xarray(
        self,
        generate_extra_variables=True,
        rename_variables=True,
    ):
        """Convert ios class to xarray dataset

        Returns:
            xarray dataset
        """

        def update_variable_index(varname, id):
            """Replace variable index (1,01,X,XX) by the given index or append
            0 padded index if no index exist in original variable name"""
            if varname.endswith(("01", "XX")):
                return f"{varname[:-2]}{id:02g}"
            elif varname.endswith(("1", "X")):
                return f"{varname[:-1]}{id}"
            return f"{varname}{id:02g}"

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

        def _handle_duplicated_variabes(variables):
            """Handle duplicated variables ios_name,units pairs and same variable
            names with different units

            Both cases aren't allowed in xarray dataset,
            so we need to rename the variables.
            """

            # Detect duplicated variables ios_name,units pairs
            duplicates = variables.duplicated(subset=["ios_name", "units"], keep=False)
            if duplicates.any():
                logger.warning(
                    "Duplicated variables(Name,Units)! The first one will be considered: %s",
                    variables.loc[duplicates][["ios_name", "units"]],
                )
                variables.drop_duplicates(
                    subset=["ios_name", "units"], keep="first", inplace=True
                )

            # Detect and rename duplicated variable names with different units
            duplicated_name = variables.duplicated(subset=["ios_name"])
            if duplicated_name.any():
                variables["var_index"] = variables.groupby("ios_name").cumcount()
                to_replace = duplicated_name & (variables["var_index"] > 0)
                new_names = variables.loc[to_replace].apply(
                    lambda x: update_variable_index(x["ios_name"], x["var_index"] + 1),
                    axis="columns",
                )
                # Update vocabulary ios_name
                for _, row in variables.loc[to_replace].iterrows():
                    for vocab in row["vocabularies"]:
                        vocab["ios_name"] = update_variable_index(
                            vocab["ios_name"], row["var_index"] + 1
                        )
                logger.info(
                    "Duplicated variable names, rename variables: %s",
                    [
                        f"{name} [{units}] -> {new_name}"
                        for (name, units), new_name in zip(
                            variables.loc[to_replace, ["ios_name", "units"]].values,
                            new_names,
                        )
                    ],
                )
                variables.loc[to_replace, "ios_name"] = new_names

            return variables

        # Retrieve the different variable attributes
        variables = pd.DataFrame(
            {
                "ios_name": self.channels["Name"],
                "units": self.channels["Units"],
                "ios_type": self.channel_details.get("Type"),
                "ios_format": self.channel_details.get("Format"),
                "pad": self.channel_details.get("Pad"),
                "vocabularies": self.vocabulary_attributes,
            }
        ).replace({"": None, "n/a": None})

        # Define data type by using the ios_type, ios_format and ios_name attributes
        variables["dtype"] = variables.apply(
            lambda x: _cast_ios_variable(x.ios_type, x.ios_format, x.ios_name),
            axis="columns",
        )

        # cast fill values to the appropriate data type
        variables["_FillValues"] = variables.apply(
            lambda x: pd.Series(x["pad"]).astype(x["dtype"]).values[0]
            if x["pad"]
            else None,
            axis="columns",
        )
        variables = _handle_duplicated_variabes(variables)

        # Parse data, assign appropriate data type, padding values
        #  and convert to xarray object
        ds = (
            pd.DataFrame.from_records(
                self.data[:, variables.index], columns=variables["ios_name"]
            )
            .replace(r"\.$", "", regex=True)
            .astype(dict(variables[["ios_name", "dtype"]].values))
            .replace(
                dict(variables[["ios_name", "_FillValues"]].dropna().values),
                value=np.nan,
            )
            .to_xarray()
        )
        ds = _flag_bad_values(ds)
        ds.attrs = self.get_global_attributes()

        # Add variable attributes
        for _, attrs in variables.iterrows():
            ds[attrs["ios_name"]].attrs = dict(
                long_name=attrs["ios_name"],
                units=attrs["units"],
                original_ios_name=attrs["ios_name"],
                original_ios_variable={
                    key: value
                    for key, value in attrs.items()
                    if key not in ("vocabularies", "dtype", "_FillValues")
                },
            )

        # Add vocabulary attributes
        #  sort matching vocabulary by rename, apply_func, sdn_parameter_name(length)
        #  and keep the first one matching for each rename outputed variables
        ds_new = xr.Dataset()
        ds_new.attrs = ds.attrs
        variables_vocabularies = (
            variables.explode("vocabularies")
            .set_index("ios_name")["vocabularies"]
            .apply(pd.Series)
            .sort_values(
                ["rename", "apply_func", "sdn_parameter_name"],
                na_position="first",
                key=lambda col: col.fillna("").str.len()
                if col.name == "sdn_parameter_name"
                else col,
            )
            .groupby("rename")
            .head(1)
        )
        for variable in ds.variables:
            if variable == "index":
                continue
            elif variable.lower().startswith(("date", "time")):
                ds_new[variable] = ds[variable]
                continue
            elif variable not in variables_vocabularies.index:
                logger.warning("Missing vocabulary for variable %s", variable)
                ds_new[variable] = ds[variable]
                continue
            variable_vocabulary = variables_vocabularies.loc[[variable]]

            if not rename_variables and not generate_extra_variables:
                ds_new[variable] = ds[variable]
                # ignore apply_func vocabularies and get the most precise vocabulary
                vocab = variable_vocabulary.query("apply_func.isna()")
                if vocab.empty:
                    continue
                ds_new[variable].attrs.update(
                    vocab.iloc[-1].drop("rename").dropna().to_dict()
                )

            if not generate_extra_variables:
                # favorize not transform variables if available
                vocab = variable_vocabulary.query("apply_func.isna()")
                if variable_vocabulary.query("apply_func.isna()").empty:
                    variable_vocabulary = variable_vocabulary.query(
                        "apply_func.isna()"
                    ).iloc[[-1]]
                else:
                    variable_vocabulary = variable_vocabulary.iloc[[-1]]

            for variable, attrs in variable_vocabulary.iterrows():
                new_var = attrs.pop("rename")

                if pd.notna(attrs["apply_func"]):
                    ufunc = eval(attrs["apply_func"], {"ds": ds, "gsw": gsw})
                    new_data = xr.apply_ufunc(ufunc, ds[variable])
                    self.add_to_history(
                        f"Generate new variable from {variable} ->"
                        f" apply {attrs['apply_func']}) -> {new_var}"
                    )
                else:
                    new_data = ds[variable]
                    self.add_to_history(
                        f"Generate new variable from {variable} -> {new_var}"
                    )
                ds_new[new_var] = (
                    ds[variable].dims,
                    new_data.data,
                    {**ds[variable].attrs, **attrs.dropna().to_dict()},
                )

        ds = ds_new

        # coordinates
        if self.obs_time:
            ds = ds.drop_vars([var for var in ds if var in ["Date", "Time"]])
            ds["time"] = (ds.dims, pd.Series(self.obs_time))
        elif self.start_dateobj:
            ds["time"] = pd.to_datetime(self.start_dateobj)
        else:
            logger.error("Unable to set time coordinate")
        ds["time"].attrs = {
            "long_name": "Time",
            "standard_name": "time",
        }

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
            ds.encoding.update(
                {"latitude": {"dtype": "float32"}, "longitude": {"dtype": "float32"}}
            )

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
                featureType = "trajectory"
            else:
                featureType = "timeSeries"
        else:
            featureType = ""
        if "depth" in ds.dims:
            featureType += "Profile" if featureType else "profile"
        ds.attrs["featureType"] = featureType
        ds.attrs["cdm_data_type"] = featureType.title()

        # Set coordinate variables
        coordinates_variables = ["time", "latitude", "longitude", "depth"]
        if any(var in ds for var in coordinates_variables):
            ds = ds.set_coords([var for var in coordinates_variables if var in ds])
            if "index" in ds.coords and ("time" in ds.coords or "depth" in ds.coords):
                ds = ds.drop_vars("index")

        return ds
