"""
Set of tools used to parsed RBR manufacturer proprieatary data formats to an
xarray data structure.
"""
import re

import pandas as pd

from ocean_data_parser.read.utils import test_parsed_dataset


def rtext(file_path, encoding="UTF-8", output=None):
    """
    Read RBR R-Text format.
    :param errors: default ignore
    :param encoding: default UTF-8
    :param file_path: path to file to read
    :return: metadata dictionary dataframe
    """
    # MON File Header end
    header_end = "NumberOfSamples"

    with open(file_path, encoding=encoding) as fid:
        line = ""
        section = "header_info"
        metadata = {section: {}}

        while not line.startswith(header_end):
            # Read line by line
            line = fid.readline()

            if re.match(r"\s*.*(=).*", line):
                key, item = re.split(r"\s*[:=]\s*", line, 1)

                # If line has key[index].subkey format
                if re.match(r".*\[\d+\]\..*", key):
                    items = re.search(r"(.*)\[(\d+)\]\.(.*)", key)
                    key = items[1]
                    index = items[2]
                    subkey = items[3].strip()

                    if key not in metadata:
                        metadata[key] = {}
                    if index not in metadata[key]:
                        metadata[key][index] = {}

                    metadata[key][index][subkey] = item.strip()

                else:
                    metadata[key] = item.strip()
            elif re.match(r"^\s+$", line):
                continue
            else:
                print(f"Ignored: {line}")
        # Read NumberOFSamples line
        metadata["number_of_samples"] = int(line.rsplit("=")[1])

        # Read data
        df = pd.read_csv(fid, sep=r"\s\s+", engine="python")

        # Make sure that line count is good
        if len(df) != metadata["number_of_samples"]:
            raise RuntimeError("Data length do not match expected Number of Samples")

        # Convert to datset
        ds = df.to_xarray()
        ds.attrs = metadata
        ds.attrs["instrument_manufacturer"] = "RBR"
        ds.attrs["instrument_model"] = metadata["Model"]
        ds.attrs["instrument_sn"] = metadata["Serial"]

        # Test parsed data
        test_parsed_dataset(ds)

        # Ouput
        if output == "dataframe":
            for var in ["instrument_manufacturer", "instrument_model", "instrument_sn"][
                ::-1
            ]:
                df.insert(0, var, ds.attrs[var])
            return df
        return ds
