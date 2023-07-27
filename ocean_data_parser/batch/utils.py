from pathlib import Path
from typing import Union
import pandas as pd
import xarray


def generate_output_path(
    ds: xarray.Dataset,
    source: str = None,
    path: Union[str, Path] = None,
    defaults: dict = None,
    file_preffix: str = "",
    file_suffix: str = "",
    output_format: str = ".nc",
) -> Path:
    """Generate output path where to save Dataset.

    Args:
        ds (xr.Dataset): Dataset
        source (str, optional): original source file path. Defaults to None.
        path (str, Path): Output path where to save the directory.
            The output path uses the python String format method to reference
            attributes accoding to the convention:
              - source_path: pathlib.Path of original parsed file filename
              - source_stem: original parsed file filename without the extension
              - global attributes: `{global_asttribute}`
              - variable attributes: `{variable_[variable]_[attribute]}`
            ex: ".\{program}\{project}\{source_stem}.nc"
        defaults (dict, optional): Placeholder for any global
            attributes or variable attributes used in output path. Defaults to None.
        file_preffix (str, optional): Preffix to add to file name. Defaults to "".
        file_suffix (str, optional): Suffix to add to file name. Defaults to "".
        output_format (str, optional): Output File Format extension.

    Returns:
        Path (Path): Generated path
    """

    # handle defaults
    original_source = Path(ds.attrs.get("source")) if ds.attrs.get("source") else None
    if source is None and original_source:
        source = str(original_source.stem)

    if source is None:
        raise RuntimeError("No output source available. Please define source output.")

    if path is None and ds.attrs.get("source"):
        path = str(Path(ds.attrs["source"]).parent)

    if isinstance(path, Path):
        path = str(path)

    # Review file_output path given by config
    path_generation_inputs = {
        **(defaults or {}),
        **{f"{key}": value for key, value in ds.attrs.items() if value},
        **{
            f"variable_{var}_{key}": value
            for var in ds.variables
            for key, value in ds[var].attrs.items()
        },
        **(
            {
                "source_path": original_source.parent,
                "source_stem": original_source.stem,
            }
            if original_source
            else {}
        ),
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
