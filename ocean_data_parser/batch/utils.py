from pathlib import Path

import xarray
import pandas as pd


def generate_output_path(
    ds: xarray.Dataset,
    source: str = None,
    path: str = ".",
    defaults: dict = None,
    file_preffix: str = "",
    file_suffix: str = "",
    output_format: str = ".nc",
) -> Path:
    """Generate output path where to save Dataset.

    Args:
        ds (xr.Dataset): Dataset
        path (str): Output path where to save the directory.
            The output path uses the python String format method to reference
            attributes accoding to the convention:
              - source_filename: pathlib.Path of original parsed file filename
              - source_filename_stem: original parsed file filename without the extension
              - global attributes: `global:{Attribute}`
              - variable attributes: `variable:{variable}:{attribute}`
            ex: ".\{global:program}\{global:project}\{source_filename.name}.nc"
        source (str, optional): original source file path. Defaults to None.
        defaults (dict, optional): Placeholder for any global
            attributes or variable attributes used in output path. Defaults to None.
        file_preffix (str, optional): Preffix to add to file name. Defaults to "".
        file_suffix (str, optional): Suffix to add to file name. Defaults to "".
        output_format (str, optional): Output File Format extension.

    Returns:
        Path (Path): Generated path
    """

    if source is None and ds.attrs.get("source"):
        source = Path(ds.attrs["source"]).stem

    if source is None:
        raise RuntimeError("No output source available. Please define source output.")

    # Review file_output path given by config
    path_generation_inputs = {
        "source_filename": source or ".",
        **(defaults or {}),
        **{f"{key}": value for key, value in ds.attrs.items() if value},
        **{
            f"variable_{var}_{key}": value
            for var in ds.variables
            for key, value in ds[var].attrs.items()
            if value
        },
        **(
            {
                "time_min": pd.to_datetime(ds["time"].min().values),
                "time_max": pd.to_datetime(ds["time"].max().values),
            }
            if "time" in ds
            else {}
        ),
    }

    # Generate path and file name
    output_path = Path(path.format(**path_generation_inputs))
    source = source.format(**path_generation_inputs)

    # Retrieve output_format if given in source

    if "." in source and not output_format:
        source, output_format = source.rsplit(".", 1)
    assert (
        output_format
    ), "Unknown output file format extension: define the format through the path or output_format inputs"

    # Generate path
    return Path(output_path) / (
        f"{file_preffix or ''}{source}{file_suffix or ''}{output_format}"
    )
