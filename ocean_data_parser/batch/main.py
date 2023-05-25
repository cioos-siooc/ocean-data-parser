import logging.config
import os
from glob import glob
from importlib import import_module
from pathlib import Path

import click
import xarray as xr
from tqdm import tqdm

from ocean_data_parser.batch.config import load_config
from ocean_data_parser.batch.registry import FileConversionRegistry
from ocean_data_parser.read import auto, utils


MODULE_PATH = Path(__file__).parent
DEFAULT_CONFIG_PATH = MODULE_PATH / "default-batch-config.yaml"

main_logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(main_logger, {"file": None})


def conversion(config=None, **kwargs):
    """Ocean Data Parser batch conversion method

    Args:
        config (dict, optional): Configuration use to run the batch conversion.
            Defaults to None.
    """
    # load config
    config = {**load_config(DEFAULT_CONFIG_PATH), **(config or {}), **kwargs}

    ## Setup logging configuration
    if config.get("logging"):
        logging.config.dictConfig(config["logging"])

    # Sentry
    if config.get("sentry", {}).get("dsn"):
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration

        sentry_logging = LoggingIntegration(
            level=config["sentry"].pop("level"),
            event_level=config["sentry"].pop("event_level"),
        )
        sentry_sdk.init(**config["sentry"], integrations=[sentry_logging])

    # Connect to database if given
    # TODO Establish connection to database

    # Load parse log file
    file_registry = FileConversionRegistry(path=config["file_registry"]).load()

    # Get Files
    to_parse = []
    for input in config["input"]:
        source_files = glob(input["path"], recursive=input.get("recursive"))
        total_files = len(source_files)
        if not config.get("overwrite"):
            # Ignore files already parsed
            file_registry.load_sources(source_files)
            source_files = file_registry.get_modified_hashes()
        to_parse += [{"files": source_files, **input}]
        logger.info("%s.%s will be parse", len(source_files), total_files)

    # Import parser module and load each files:
    for input in to_parse:
        parser = input.get("parser", "auto")

        if parser == "auto":
            parser_func = auto.file
        else:
            logging.info("Load parser %s", input["parser"])
            # Load the appropriate parser and read the file
            read_module, filetype = input["parser"].rsplit(".", 1)
            try:
                mod = import_module(f"ocean_data_parser.read.{read_module}")
                parser_func = getattr(mod, filetype)
            except Exception:
                logger.exception("Failed to load module %s", parser)
                return
        for file in tqdm(input["files"], desc="Run batch conversion", unit="file"):
            try:
                logger.extra[file] = file
                output_file = _convert_file(file, parser_func, config)
                file_registry.update_source(file)
                file_registry.add_to_source(file, output_file=output_file)
            except Exception as error:
                logger.exception("Conversion failed")
                file_registry.add_to_source(file, error_message=error)

        file_registry.save()


def _convert_file(file: str, parser: str, config: dict) -> str:
    # parse file to xarray
    logger.extra["file"] = file
    ds = parser(file)

    # Update global and variable attributes from config
    ds.attrs.update(config.get("global_attributes"))
    if file in config.get("attribute_corrections"):
        ds.attrs.update(config["attribute_corrections"][file])

    for var, attrs in config.get("variable_attributes").items():
        if var in ds:
            ds[var].attrs.update(attrs)

    for pipe in config["xarray_pipe"]:
        ds = ds.pipe(*pipe)
        # TODO add to history

    # IOOS QC
    # TODO add ioos_qc

    # Manual QC
    # TODO add manual flags from external source

    # Aggregate flags
    # TODO aggregate ioos_qc and manual flags

    # Standardize output
    ds = utils.standardize_dataset(ds)

    # Save to
    output_path = None
    if config.get("file_output") and "path":
        overwrite = config["file_output"].pop("overwrite", False)
        output_path = _generate_output_path(ds, source=file, **config["file_output"])
        if output_path.exists() and not overwrite:
            logger.info(
                "Converted output file already exist and won't be overwritten. (output_path=%s)",
                output_path,
            )
            return output_path

        elif not output_path.parent.exists():
            logger.info("Create new directory: %s", output_path.parent)
            output_path.parent.mkdir()
        logger.info("Save to: %s", output_path)
        ds.to_netcdf(output_path)

    if config.get("upload_to_database"):
        # TODO update to database
        # ds.to_dataframe()
        pass

    return output_path


def _generate_output_path(
    ds: xr.Dataset,
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
