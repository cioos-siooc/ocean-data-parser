from pathlib import Path

import xarray


def _generate_output_path(
    ds: xarray.Dataset,
    path: str,
    source: str = None,
    defaults: dict = None,
    file_preffix: str = "",
    file_suffix: str = "",
    output_format: str = None,
) -> Path:
    """Generate output path where to save Dataset.

    Args:
        ds (xr.Dataset): Dataset
        path (str): Output path where to save the directory.
            The output path uses the python String format method to reference
            attributes accoding to the convention:
              - source_filename: pathlib.Path of original parsed file filename
              - source_filename_stem: original parsed file filename without the extension
              - global attributes: `global_{Attribute}`
              - variable attributes: `variable_{variable}_{attribute}`
            ex: ".\{global_program}\{global_project}\{source_filename.name}.nc"
        source (str, optional): original source file path. Defaults to None.
        defaults (dict, optional): Placeholder for any global
            attributes or variable attributes used in output path. Defaults to None.
        file_preffix (str, optional): Preffix to add to file name. Defaults to "".
        file_suffix (str, optional): Suffix to add to file name. Defaults to "".
        output_format (str, optional): Output File Format extension.

    Returns:
        Path: _description_
    """

    def _add_preffix_suffix(filename: Path):
        return Path(filename.parent) / (
            (file_preffix or "") + filename.stem + (file_suffix or "") + filename.suffix
        )

    output_format = output_format or Path(path or ".").suffix
    assert (
        output_format
    ), "Unknown output file format extension: define the format through the path or output_format inputs"

    if path is None and source:
        return _add_preffix_suffix(Path(f"{source}{output_format}"))

    defaults = defaults or {}
    # Review file_output path given by config
    path_generation_inputs = {
        "source_filename": Path(source or "."),
        **defaults,
        **{f"global_{key}": value for key, value in ds.attrs.items()},
        **{
            f"variable_{var}_{key}": value
            for var in ds
            for key, value in ds[var].attrs.items()
        },
        **{
            f"variable_{var}_{key}": value
            for var in ds.coords
            for key, value in ds[var].attrs.items()
        },
    }

    output_path = Path(path.format(**path_generation_inputs))
    if output_path.suffix != output_format:
        output_path += output_format
    if not output_path.name:
        output_path = output_path / Path(source).stem + output_format
    return _add_preffix_suffix(output_path)
