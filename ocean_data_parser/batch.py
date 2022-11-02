from importlib import import_module
import logging
from glob import glob
import yaml
import os
import re

import pandas as pd
import xarray as xr
from tqdm import tqdm


from ocean_data_parser.read import utils, auto

empty_file_registry = {"path": None, "last_update": None}

logger = logging.getLogger(__name__)
adapted_logger = logging.LoggerAdapter(logger, {"file": None})


def load_config(config_path: str = None, encoding="UTF-8"):
    """Load YAML configuration file, if not provided load default configuration."""
    # Get default config if no file provided
    if config_path is None:
        config_path = os.path.join(
            os.path.dirname(__file__), "default-batch-config.yaml"
        )

    with open(config_path, "r", encoding=encoding) as file:
        config = yaml.load(file, Loader=yaml.SafeLoader)

    return config


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
    file_registry = _load_registry(config)

    # Get Files
    for id, input in enumerate(config["input"]):
        config[id]["files"] = glob(input["path"])
        total_files = len(config[id]["files"])
        if not config["overwrite"]:
            # Ignore files already parsed
            config[id]["files"] = [
                file
                for file in config[id]["files"]
                if file not in file_registry.index
                or os.path.getmtime(file) > file_registry.loc[file, "last_update"]
            ]
        logger.info("%s.%s will be parse", len(config[id]["file"]), total_files)

    # Import parser module and load each files:
    for input in config["inputs"]:
        parser = input.get("parser", "auto")

        if parser == "auto":
            parser_func = auto.file
        else:
            logging.info("Load parser %s", input["parser"])
            # Load the appropriate parser and read the file
            read_module, filetype = config["parser"].rsplit(".", 1)
            try:
                mod = import_module(f"ocean_data_parser.read.{read_module}")
                parser_func = getattr(mod, filetype)
            except Exception:
                logger.exception("Failed to load module %s", parser)
                return
        for file in input["files"]:
            try:
                _convert_file(file, parser_func, config)
            except Exception as e:
                file_registry.loc[file, "error_message"] = e


def _load_registry(config):

    file_registry_exists = os.path.exists(config["file_registry"])
    if config.get("file_registry") is None or not file_registry_exists:
        file_registry = pd.DataFrame(empty_file_registry, index=["path"])
    elif config["file_registry"].endswith("csv"):
        file_registry = pd.read_csv(config["file_registry"], index_col=["path"])
    elif config["file_registry"].endswith("parquet"):
        file_registry = pd.read_parquet(config["file_registry"], index_col=["path"])
    return file_registry


def _update_registry(file_registry, config):

    if config["file_registry"].endswith("csv"):
        pd.write_csv(config["file_registry"])
    elif config["file_registry"].endswith("parquet"):
        pd.write_parquet(config["file_registry"])
    else:
        logger.error("Unknown registry format")


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
        "/Users/jessybarrette/repo/ocean-data-parser-start/ocean_data_parser/default-batch-config.yaml",
        encoding="UTF-8",
    ) as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
    convert(config)
