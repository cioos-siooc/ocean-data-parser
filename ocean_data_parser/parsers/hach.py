import pandas as pd

from ocean_data_parser.parsers.utils import standardize_dataset


def txt(
    file: str,
    global_attributes: dict = None,
    encoding: str = "utf-16LE",
    timezone: str = "UTC",
):
    """Parse HACH TR9100 Portable Spectrophotometer txt file format.
    Args:
        file (str): Path to file
        global_attributes (dict): Global attributes to add to the dataset
        encoding (str): File encoding
        timezone (str): Timezone to localize the time column
    Returns:
        xarray.Dataset: Dataset with the parsed data
    """
    with open(file, encoding=encoding) as f:
        device_type_label, device_type = f.readline().split(",", 1)
        device_id_label, device_id = f.readline().split(",", 1)
        columns = f.readline().strip().lower().replace(" ", "_").split(",")

        if "Device Type" not in device_type_label:
            raise ValueError("Device Type not found in file")
        if "Serial Number" not in device_id_label:
            raise ValueError("Serial Number not found in file")

        df = pd.read_csv(
            f, sep=",", encoding=encoding, names=columns, header=None, index_col=False
        )

    # Format time column
    df["time"] = pd.to_datetime(df["time"], format="%d-%b-%Y %H:%M:%S").dt.tz_localize(
        timezone
    )

    # Convert to xarray dataset
    ds = df.to_xarray()
    ds.attrs = {
        **(global_attributes or {}),
        "device_type": device_type,
        "device_id": device_id,
    }
    return standardize_dataset(ds)
