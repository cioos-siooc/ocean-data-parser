"""
RBR Ltd. is a company that specializes in oceanographic instruments and sensors.

They provide a range of instruments for measuring various parameters in the ocean,
including temperature, salinity, pressure, and more.
"""

import re

import pandas as pd
from loguru import logger
from xarray import Dataset

from ocean_data_parser.parsers.utils import standardize_dataset


def rtext(
    file_path: str,
    encoding="UTF-8",
    header_end: str = "NumberOfSamples",
    errors: str = "raise",
) -> Dataset:
    """Read RBR legacy R-Text Engineering format.

    Args:
        file_path (Path): RBR R-Text file path
        encoding (str, optional): File encoding. Defaults to "UTF-8".
        header_end (str, optional): End of the metadata header.
            Defaults to "NumberOfSamples".
        errors (str, optional): Error handling. Defaults to "raise".

    Raises:
        RuntimeError: File length do not match expected Number of Samples

    Returns:
        Dataset: Parsed Dataset
    """
    line = ""
    metadata = {}
    with open(file_path, encoding=encoding) as fid:
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
            if errors == "raise":
                raise RuntimeError(
                    "Data length do not match expected Number of Samples"
                )
            else:
                logger.warning("Data length do not match expected Number of Samples")

        # Convert to datset
        ds.attrs = {
            **metadata,
            "instrument_manufacturer": "RBR",
            "instrument_model": metadata["Model"],
            "instrument_sn": metadata["Serial"],
        }

        ds = standardize_dataset(ds)
        return ds
