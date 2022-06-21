import pandas as pd
import re
import logging
from .utils import standardize_dateset

logger = logging.getLogger(__name__)

MAXIMUM_TIME_DIFFERENCE_IN_SECONDS = 300

notes_dtype_mapping = {
    "day_of_year": float,
    "note_type": str,
    "Smpl_Intrvl": float,
    "1st_std_delay": float,
    "File_duration": float,
    "Std_time1": float,
    "num_Stds": float,
    "min_btw_stds": float,
}


def _format_variables(name):
    name = re.sub("\(|\)", "_", name)
    name = re.sub("_$", "", name)
    return name


def superCO2(path, output=None):
    """Parse superCO2 output file txt file"""
    header = []
    line = 1
    with open(path) as f:
        header += [f.readline()]
        if re.search("\d+ header lines", header[0]):
            n_header_lines = int(re.search("(\d+) header lines", header[0])[1])
        else:
            logger.error("Unknown header format")

        # Read the rest of the header lines
        while line < n_header_lines - 1:
            header.append(f.readline())
            line += 1

        # Read the column header and data with pandas
        df = pd.read_csv(f, sep="\t", dtype={"Date": str, "Time": str})
    if "Collected beginning on" in header[2]:
        collected_beginning_date = pd.to_datetime(header[3])
    else:
        collected_beginning_date = pd.NaT
    # Reformat variable names
    df.columns = [_format_variables(var) for var in df.columns]

    # Generate time variable from Date and Time columns
    df["time"] = pd.to_datetime((df["Date"] + " " + df["Time"]), format="%Y%m%d %H%M%S")

    # Review day of the year variable
    df["time_doy_utc"] = pd.to_datetime(
        df["DOY_UTC"] - 1,
        unit="D",
        origin=pd.Timestamp(collected_beginning_date.year, 1, 1),
    )

    # Compare DOY_UTC vs Date + Time
    dt = (df["time"] - df["time_doy_utc"]).mean().total_seconds()
    dt_std = (df["time"] - df["time_doy_utc"]).std().total_seconds()
    if dt > MAXIMUM_TIME_DIFFERENCE_IN_SECONDS:
        logger.warning(
            f"Date + Time and DOY_UTC variables have an average time difference of {dt}s>{MAXIMUM_TIME_DIFFERENCE_IN_SECONDS}s with a standard deviation of {dt_std}s"
        )

    global_attributes = {
        "title": header[1].replace("\n", ""),
        "collected_beginning_date": collected_beginning_date,
    }

    if output == "dataframe":
        return df, global_attributes

    # Convert to an xarray dataset
    ds = df.to_xarray()
    ds.attrs = global_attributes

    return standardize_dateset(ds)


def superCO2_notes(path):
    """Parse superCO2 notes files and return a pandas dataframe"""
    line = True
    notes = []
    with open(path, "r") as f:
        while line:
            line = f.readline()
            if line in (""):
                continue
            elif re.match("\d\d\d\d\/\d\d\/\d\d \d\d\:\d\d\:\d\d\s+\d+\.\d*", line):
                # Parse time row
                note_ensemble = re.match(
                    "(?P<time>\d\d\d\d\/\d\d\/\d\d \d\d\:\d\d\:\d\d)\s+(?P<day_of_year>\d+\.\d*)",
                    line,
                ).groupdict()
                # type row
                note_ensemble["note_type"] = f.readline().replace("\n", "")
                # columns and data
                header = f.readline().replace("\n", "")
                columns = re.split("\s+", header)
                line = f.readline().replace("\n", "")
                data = re.split("\s+", line)

                # Combine notes to previously parsed ones
                notes += [
                    {
                        **note_ensemble,
                        **{col: value for col, value in zip(columns, data)},
                    }
                ]
    # Convert notes to a dataframe
    df = pd.DataFrame.from_dict(notes)
    df["time"] = pd.to_datetime(df["time"])
    df = df.astype(dtype=notes_dtype_mapping, errors="ignore")
    return df
