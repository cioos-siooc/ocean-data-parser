"""
# RBR Ltd.
<https://rbr-global.com/>

"""
import re

import pandas as pd

from ocean_data_parser.parsers.utils import standardize_dataset


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
        ds = pd.read_csv(fid, sep=r"\s\s+", engine="python").to_xarray()

        # Make sure that line count is good
        if ds.dims["index"] != metadata["number_of_samples"]:
            raise RuntimeError("Data length do not match expected Number of Samples")

        # Convert to datset
        ds.attrs = {
            **metadata,
            "instrument_manufacturer": "RBR",
            "instrument_model": metadata["Model"],
            "instrument_sn": metadata["Serial"],
        }

        ds = standardize_dataset(ds)
        return ds
