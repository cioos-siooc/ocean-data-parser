import logging
import os
import shutil
import sys
from glob import glob
from importlib import import_module
from multiprocessing import Pool
from pathlib import Path

import click
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm
from xarray import Dataset

from ocean_data_parser import geo, process
from ocean_data_parser._version import __version__
from ocean_data_parser.batch.config import load_config
from ocean_data_parser.batch.registry import FileConversionRegistry
from ocean_data_parser.batch.utils import generate_output_path
from ocean_data_parser.read import auto, utils

MODULE_PATH = Path(__file__).parent
DEFAULT_CONFIG_PATH = MODULE_PATH / "default-batch-config.yaml"

# Set logging configuration
load_dotenv(".env")
logger.configure(extra={"source_file": ""})
logger.remove()
logger.add(
    sys.stderr,
    level=os.getenv("LOGURU_LEVEL", "INFO"),
    format='<level>{level.icon}</level> <blue>"{file.path}"</blue>: <yellow>line {line}</yellow> | <cyan>"{extra[source_file]}"</cyan> - <level>{message}</level>',
)
logger.add(
    "ocean_data_parser.log",
    level=os.getenv("LOGURU_LEVEL", "WARNING"),
    format="{time}|{level}|{file.path}:{line}| {extra[source_file]} - {message}",
)


# Redirect logging loggers to loguru
class InterceptHandler(logging.Handler):
    """
    Add logging handler to augment python stdlib logging.

    Logs which would otherwise go to stdlib logging are redirected through
    loguru.
    # https://stackoverflow.com/questions/66769431/how-to-use-loguru-with-standard-loggers
    """

    @logger.catch(default=True, onerror=lambda _: sys.exit(1))
    def emit(self, record):
        # Get corresponding Loguru level if it exists.
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


logging.basicConfig(
    handlers=[InterceptHandler()], level=os.getenv("LOGURU_LEVEL", "INFO")
)
classic_logger = logging.getLogger()


@click.command()
@click.option(
    "--config", "-c", type=click.Path(exists=True), help="Path to configuration file"
)
@click.option(
    "--new_config",
    type=click.Path(),
    help="Generate a new configuration file at the given path",
)
def cli_files(config=None, new_config=None):
    if new_config:
        new_config = Path(new_config)
        logger.info(
            "Copy a default config to given path {} to {}",
            DEFAULT_CONFIG_PATH,
            new_config,
        )
        if not new_config.parent.exists():
            logger.info("Generate new directory")
            new_config.parent.mkdir(parents=True)
        shutil.copy(DEFAULT_CONFIG_PATH, new_config)
        return

    logger.info("Run config={}", config)
    main(config=config)


def main(config=None, **kwargs):
    """Ocean Data Parser batch conversion method

    Args:
        config (dict, optional): Configuration use to run the batch conversion.
            Defaults to None.
        **kwargs (optiona): Overwrite any configuration parameter by
            matching first level key.
    """
    logger.info("Run ocean-data-parser[{}] batch conversion", __version__)

    # load config
    config = {
        **load_config(DEFAULT_CONFIG_PATH),
        **(load_config(config) if isinstance(config, str) else config or {}),
        **kwargs,
    }


    # Load file registry
    logger.debug("Load file registry")
    file_registry = FileConversionRegistry(**config["registry"])

    # Get Files
    to_parse = []
    logger.info("Compile files to parse")
    for input_path, parser in zip(
        config["input_path"].split(","), config["parser"].split(",")
    ):
        logger.info("Search files: '{}'", input_path)
        source_files = glob(input_path, recursive=config.get("recursive"))
        total_files = len(source_files)
        logger.info("{} files detected", len(source_files))
        logger.info(
            "Add {} unknown files to registry",
            len(
                [file for file in source_files if file not in file_registry.data.index]
            ),
        )
        file_registry.add(source_files)

        # Ignore files already parsed
        logger.info(
            "Compare files with registry hashes and ignore already parsed files"
        )
        source_files = file_registry.get_source_files_to_parse(
            overwrite=config.get("overwrite", "False")
        )
        if not source_files:
            continue
        to_parse += [
            {"files": source_files, "input_path": input_path, "parser": parser}
        ]
        logger.info("Detected {}/{} needs to be parse", len(source_files), total_files)

    if not to_parse:
        logger.info("No files need to be parsed")
        return

    # Import parser modules and load each files:
    for input in to_parse:
        parser = input["parser"]
        logger.info("Load parser={}", parser)
        if parser == "auto":
            parser_func = auto.file
        else:
            # Load the appropriate parser and read the file
            read_module, filetype = input["parser"].rsplit(".", 1)
            try:
                mod = import_module(f"ocean_data_parser.read.{read_module}")
                parser_func = getattr(mod, filetype)
            except ImportError:
                logger.exception("Failed to load module {}", parser)
                return

        inputs = [(file, parser_func, config) for file in input["files"]]
        tqdm_parameters = dict(unit="file", total=len(input["files"]))
        if config.get("multiprocessing"):
            logger.info(
                "Run conversion in parallel with multiprocessing on {} files",
                len(inputs),
            )
            n_workers = (
                config["multiprocessing"]
                if isinstance(config["multiprocessing"], int)
                else None
            )
            with Pool(n_workers) as pool:
                response = list(
                    tqdm(
                        pool.imap(_convert_file, inputs),
                        **tqdm_parameters,
                        desc=f"Run parallel batch conversion with {n_workers or 'All'} workers",
                    )
                )

        else:
            logger.info("Run conversion on {} files", len(inputs))
            response = []
            for item in tqdm(
                inputs,
                **tqdm_parameters,
                desc="Run batch conversion",
            ):
                response += [_convert_file(item)]

        # Update registry
        logger.info("Update file registry")
        for source, output_path, error_message in response:
            if output_path:
                file_registry.update_fields(source, output_path=output_path)
            else:
                with logger.contextualize(source_file=source):
                    logger.error("Failed conversion: {}", error_message)
                file_registry.update_fields(source, error_message=error_message)
        logger.info("Save file registry")
        file_registry.save()

    logger.info("Conversion completed")
    return file_registry


def _convert_file(args):
    with logger.contextualize(source_file=args[0]):
        try:
            output_file = convert_file(args[0], args[1], args[2])
            return (args[0], output_file, None)
        except Exception as error:
            if args[2].get("errors") == "raise":
                raise error
            logger.exception("Conversion failed")
            return (args[0], None, error)


def convert_file(file: str, parser: str, config: dict) -> str:
    """Parse file with given parser and configuration

    Args:
        file (str): file path
        parser (str): ocean_data_parser.read parser.
        config (dict): Configuration use to apply the conversion

    Returns:
        str: _description_
    """

    def _get_file_attributes():
        file_attributes = config.get("file_specific_attributes")
        if not file_attributes or file not in file_attributes:
            return {}
        return config["file_specific_attributes"].loc[file].dropna().to_dict()

    def _get_mapped_global_attributes(
        mapping: pd.DataFrame = None, by: list = None, log_level="WARNING"
    ):
        if mapping is None and by is None:
            return {}

        query = " and ".join(
            [f"( {attr} == {ds.attrs.get(attr)} or {attr}.isna() )" for attr in by]
        )
        matched_mapping = mapping.query(query)
        if matched_mapping.empty and log_level:
            logger.log(log_level, "No mapping match exist for global attributes: ")
            return {}

        # Regroup all matched rows within a single dictionary
        return {
            k: v
            for row in matched_mapping.iterrows()
            for k, v in row.dropna().to_dict()
        }

    # Parse file to xarray
    logger.debug("Parse file: {}", file)
    ds = parser(file)
    if not isinstance(ds, Dataset):
        raise RuntimeError(
            f"{parser.__module__}{parser.__name__}:{file} didn't return an Xarray Dataset"
        )

    # Update global and variable attributes from config
    ds.attrs.update(
        {
            **config.get("global_attributes", {}),
            **_get_file_attributes(),
            "source": file,
        }
    )
    for var, attrs in config.get("variable_attributes").items():
        if var in ds:
            ds[var].attrs.update(attrs)

    # Attribute Corrections
    ds.attrs.update(_get_mapped_global_attributes())

    # Add Geospatial Attributes
    if config.get("geographical_areas") and "latitude" in ds and "longitude" in ds:
        ds.attrs["geographical_areas"] = geo.get_geo_code(
            (ds["longitude"], ds["latitude"]), config["geographical_areas"]["regions"]
        )
    if (
        config.get("reference_stations").get("path")
        and "latitude" in ds
        and "longitude" in ds
    ):
        ds.attrs["reference_stations"] = geo.get_nearest_station(
            ds["longitude"],
            ds["latitude"],
            config["reference_stations"]["stations"],
            config["reference_stations"]["maximum_distance_from_reference_station_km"],
        )

    # Processing
    for pipe in config["xarray_pipe"]:
        ds = ds.pipe(*pipe)
        # TODO add to history

    # IOOS QC
    if config.get("ioos_qc"):
        ds = ds.process.ioos_qc(config["ioos_qc"])
    # TODO add ioos_qc

    # Manual QC
    # TODO add manual flags from external source

    # Aggregate flags
    # TODO aggregate ioos_qc and manual flags

    # Standardize output
    ds = utils.standardize_dataset(ds)

    # Save to
    output_path = None
    if config.get("file_output").get("path"):
        output_path = generate_output_path(ds, **config["file_output"])
        if not output_path.parent.exists():
            logger.info("Create new directory: {}", output_path.parent)
            output_path.parent.mkdir(parents=True)
        logger.trace("Save to: {}", output_path)
        ds.to_netcdf(output_path)

    if config.get("upload_to_database"):
        # TODO update to database
        # ds.to_dataframe()
        pass

    return output_path


if __name__ == "__main__":
    cli_files()
