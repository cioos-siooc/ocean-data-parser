import re
from io import StringIO
from pathlib import Path
from typing import Union

import pandas as pd
import xarray
from loguru import logger


def generate_output_path(
    ds: xarray.Dataset,
    path: Union[str, Path] = None,
    file_name: str = None,
    file_preffix: str = "",
    file_suffix: str = "",
    output_format: str = ".nc",
    defaults: dict = None,
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
    if file_name is None and original_source:
        file_name = str(original_source.stem)
    elif file_name is None:
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
    file_name = file_name.format(**path_generation_inputs)

    # Retrieve output_format if given in source

    if "." in file_name and not output_format:
        file_name, output_format = file_name.rsplit(".", 1)
    assert (
        output_format
    ), "Unknown output file format extension: define the format through the path or output_format inputs"

    # Generate path
    return Path(output_path) / (
        f"{file_preffix or ''}{file_name}{file_suffix or ''}{output_format}"
    )


class VariableLevelLogger:
    def __init__(
        self,
        level,
        format="{level}|{file.path}:{line} - {message}",
        backtrace=False,
        filter=None,
    ):
        self.io = StringIO()
        self.level = level
        self.id = logger.add(
            self.io,
            level=level,
            format=format,
            backtrace=backtrace,
            filter=filter or self._level_filter(level),
        )

    def values(self):
        value = self.io.getvalue()
        if self.level != "ERROR" or value == "" and "Traceback" in value:
            return value
        value = value.split("  File")[-1]
        return re.sub(r"\s+", " ", value.replace("\n", ""))

    def close(self):
        logger.remove(self.id)
        self.io.close()

    def _level_filter(self, level):
        def is_level(record):
            return record["level"].name == level

        return is_level
