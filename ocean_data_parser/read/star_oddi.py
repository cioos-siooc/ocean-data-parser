import pandas as pd
import re

import logging
logger = logging.getLogger(__name__)

def DAT(path, encoding="cp1252", kwargs_read_csv=None):
    def _standardize_attributes(item):
        item = re.sub(r"[\.\:]", "", item.strip().lower())
        return re.sub(r"\s", "_", item)

    if kwargs_read_csv is None:
        kwargs_read_csv = {}
    metadata = {}
    variables = {}
    with open(path, "r", encoding=encoding) as f:
        line = "#"

        # Loop through the header lines
        while line.startswith("#"):
            line = f.readline()
            if not line.startswith("#"):
                break

            _, attr, value = line.strip().split("\t", 2)
            if attr.strip() == "Axis":
                axis = line.strip().split("\t")
                name = re.search(r"(?P<name>[^\(]+)\((?P<units>.+)\)", axis[3])

                variables[_standardize_attributes(name["name"])] = {
                    "long_name": name["name"],
                    "units": name["units"],
                }
            elif attr.strip() == "Series":
                pass
            else:
                metadata[_standardize_attributes(attr)] = value.strip()

        if metadata["date_&_time"] == "1":
            variables = {**{"time": {}}, **variables}

        # TODO parse recorder info
        # TODO rename attributes to cf standard
        # TODO parse data line to review time range and n_records
        # TODO add some logging info
        
        # Parse data
        df = pd.read_csv(
            f,
            header=None,
            sep="\t",
            decimal=metadata["decimal_point"],
            names=variables.keys(),
            parse_dates=["time"],
        )
        if "time" in df:
            df = df.set_index(["time"])
        
        # Convert to xarray object and add related metadata
        ds = df.to_xarray()
        ds.attrs = metadata
        for var in ds:
            ds[var].attrs = variables[var]
        return ds
