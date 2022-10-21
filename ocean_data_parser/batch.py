from asyncore import file_dispatcher
import importlib
import logging
from glob import glob
import yaml
import os
import re

import pandas as pd
import xarray as xr
from tqdm import tqdm

from ocean_data_parser.read import utils

empty_file_registry = {"path": None, "last_update": None}

logger = logging.getLogger(__name__)
adapted_logger = logging.LoggerAdapter(logger, {"file": None})


def convert(config):
    ## Setup logging configuration
    if config["log"]["file"]["path"]:
        file_handler = logging.FileHandler(config["log"])
        file_handler.setFormatter(config["log"]["file"]["format"])
        file_handler.setLevel(config["log"]["file"]["level"])
        adapted_logger.add_handler(file_handler)

    # Sentry
    if config["log"]["sentry"]["dsn"]:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration

        sentry_logging = LoggingIntegration(
            level=config["sentry"].pop("level"),
            event_level=config["sentry"].pop("event_level"),
        )
        sentry_sdk.init(**config["sentry"], integrations=[sentry_logging])

    # Connect to database if given
    # TODO add connection to database

    # Load parse log file
    file_registry_exists = os.path.exists(config["file_registry"])
    if config.get("file_registry") is None or not file_registry_exists:
        file_registry = pd.DataFrame(empty_file_registry, index=["path"])
    elif config["file_registry"].endswith("csv"):
        file_registry = pd.read_csv(config["file_registry"], index_col=["path"])

    # Define Input
    files = glob(config["input"])
    if not config["overwrite"]:
        # Ignore files already parsed
        files = [
            file
            for file in files
            if file not in file_registry.index
            or os.path.getmtime(file) > file_registry.loc[file, "last_update"]
        ]

    # Get parser
    if isinstance(config["parser"], str) and "." in config["parser"]:
        logging.info("Load parser %s", config["parser"])
        module, func = config["parser"].rsplit(".", 1)
        parser = getattr(
            importlib.import_module(f"ocean_data_parser.read.{module}"), func
        )
    else:
        import ocean_data_parser.read

        parser = ocean_data_parser.read.file

    # Parse files
    tbar = tqdm(files, desc="Batch convert files", unit="file")
    for file in tbar:
        _convert_file(file, parser, config)


def _convert_file(file, parser, config):
    # parse file to xarray
    ds = parser(file)

    # Rename variables
    # ds = ds.rename(config["rename_variables"])

    # Merge Metadata
    # merge_steps = {
    #     key: value
    #     for key, value in config.items()
    #     if key in ("merge", "merge_asof")
    # }

    # for merge_type, inputs in merge_steps.items():
    #     if merge_type == 'merge':
    #         ds = ds.merge(config[])
    ## By SerialNumber

    # IOOS QC

    # Manual QC

    # Aggregate flags

    # Run Sentry warnings

    # Standardize output
    ds = utils.standardize_dataset(ds)

    # Save to
    if config.get("file_output"):
        output_path = _generate_output_path(file, ds, config)
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            logger.info("Create new directory: %s", output_dir)
            os.makedirs(output_dir)
        logger.info("Save to: %s", output_path)
        ds.to_netcdf(output_path)

    if config.get("upload_to_database"):
        # TODO update to database
        # ds.to_dataframe().to_sql()
        pass


def _generate_output_path(source_file_path, ds, config):
    def _add_prefix_suffix(full_path):
        path, file_ext = os.path.split(full_path)
        file, ext = os.path.splitext(file_ext)
        return os.path.join(
            path,
            config["file_output"].get("file_prefix")
            or "" + file + config["file_output"].get("file_suffix")
            or "" + ext,
        )

    if config["file_output"] is None:
        return None
    elif config["file_output"]["path"] is None:
        return _add_prefix_suffix(source_file_path) + ".nc"

    # Review file_output path given by config
    output_path = config["file_output"]["path"]
    if "$$" in output_path:
        path_variables = re.search(r"$$([^$]+)$$", output_path)
        for path_variable in path_variables:
            if path_variable.startswith("global:"):
                value = ds.attrs[path_variable[6:]]
            elif path_variable in ds:
                value = ds[path_variable].values

            output_path = output_path.replace("$${path_variable}$$", value)
    return _add_prefix_suffix(output_path)


if __name__ == "__main__":
    with open(
        "/Users/jessybarrette/repo/ocean-data-parser-start/ocean_data_parser/sample-batch-config.yaml"
    ) as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
    convert(config)
