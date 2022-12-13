import logging
import re

import pandas as pd

from .utils import standardize_dataset

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
superCO2_dtypes = {
    "DOY_UTC": float,
    "CO2_ppm": float,
    "CO2_abs": float,
    "H2O_ppt_mass": float,
    "H2Oabs": float,
    "Cell_T": float,
    "Cell_P": float,
    "820pwr": float,
    "Fluke_Temp": float,
    "Press(kPa)": float,
    "SB63_T": float,
    "SB63_O2": float,
    "SB63_RawP": float,
    "SB63_RawTV": float,
    "Press(V)": object,
    "IO1": float,
    "IO2": float,
    "IO3": float,
    "Valve1pos": int,
    "StandardVal": int,
    "TSG_T": float,
    "TSG_Cond": float,
    "TSG_Sal": float,
    "Remote_Therm": float,
    "Date": str,
    "Time": str,
}


def _format_variables(name):
    name = re.sub(r"\(|\)", "_", name)
    name = re.sub(r"_$", "", name)
    return name


def superCO2(path, output=None):
    """Parse superCO2 output file txt file"""
    header = []
    line = 1
    with open(path, encoding="utf-8") as f:
        header += [f.readline()]
        if re.search(r"\d+ header lines", header[0]):
            n_header_lines = int(re.search(r"(\d+) header lines", header[0])[1])
        else:
            logger.error("Unknown header format")

        # Read the rest of the header lines
        while line < n_header_lines - 1:
            header.append(f.readline())
            line += 1

        # Read the column header and data with pandas
        df = pd.read_csv(
            f,
            sep=r"\t",
            engine="python",
            dtype=superCO2_dtypes,
            na_values=[-999, "NaN"],
        )
    if "Collected beginning on" in header[2]:
        collected_beginning_date = pd.to_datetime(header[3])
    else:
        collected_beginning_date = pd.NaT
    # Reformat variable names
    df.columns = [_format_variables(var) for var in df.columns]

    # Generate time variable from Date and Time columns
    df["time"] = (
        pd.to_datetime(
            (df["Date"] + " " + df["Time"]), format="%Y%m%d %H%M%S", utc=True
        )
        .dt.tz_convert(None)
        .dt.to_pydatetime()
    )

    # Review day of the year variable
    df["time_doy_utc"] = (
        pd.to_datetime(
            df["DOY_UTC"] - 1,
            unit="D",
            origin=pd.Timestamp(collected_beginning_date.year, 1, 1),
            utc=True,
        )
        .dt.tz_convert(None)
        .dt.to_pydatetime()
    )

    # Compare DOY_UTC vs Date + Time
    dt = (df["time"] - df["time_doy_utc"]).mean().total_seconds()
    dt_std = (df["time"] - df["time_doy_utc"]).std().total_seconds()
    if dt > MAXIMUM_TIME_DIFFERENCE_IN_SECONDS:
        logger.warning(
            "Date + Time and DOY_UTC variables have an average time difference of %ss>%ss with a standard deviation of %ss",
            dt,
            MAXIMUM_TIME_DIFFERENCE_IN_SECONDS,
            dt_std,
        )

    global_attributes = {
        "title": header[1].replace(r"\n", ""),
        "collected_beginning_date": collected_beginning_date,
    }

    if output == "dataframe":
        return df, global_attributes

    # Convert to an xarray dataset
    ds = df.to_xarray()
    ds.attrs = global_attributes

    return standardize_dataset(ds)


def superCO2_notes(path):
    """Parse superCO2 notes files and return a pandas dataframe"""
    line = True
    notes = []
    with open(path, "r", encoding="utf-8") as f:
        while line:
            line = f.readline()
            if line in (""):
                continue
            elif re.match(r"\d\d\d\d\/\d\d\/\d\d \d\d\:\d\d\:\d\d\s+\d+\.\d*", line):
                # Parse time row
                note_ensemble = re.match(
                    r"(?P<time>\d\d\d\d\/\d\d\/\d\d \d\d\:\d\d\:\d\d)\s+(?P<day_of_year>\d+\.\d*)",
                    line,
                ).groupdict()
                # type row
                note_ensemble["note_type"] = f.readline().replace("\n", "")
                # columns and data
                header = f.readline().replace("\n", "")
                columns = re.split(r"\s+", header)
                line = f.readline().replace("\n", "")
                data = re.split(r"\s+", line)

                # Combine notes to previously parsed ones
                notes += [{**note_ensemble, **dict(zip(columns, data))}]
    # Convert notes to a dataframe
    df = pd.DataFrame.from_dict(notes)
    df["time"] = pd.to_datetime(df["time"]).dt.to_pydatetime()
    df = df.astype(dtype=notes_dtype_mapping, errors="ignore")
    return df.to_xarray()
