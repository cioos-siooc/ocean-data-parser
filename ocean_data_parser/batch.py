import importlib
import logging
from glob import glob
import yaml
import os
import re

import pandas as pd

empty_file_registry = {"path": None, "last_update": None}

logger = logging.getLogger(__name__)
adapted_logger = logging.LoggerAdapter(logger, {"file": None})


def convert(config):
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

    for file in files:
        current_file_timestamp = os.path.getmtime(file)
        if (
            file in file_registry.index
            and file_registry.loc[file]["last_update"] < current_file_timestamp
            and not config["overwrite"]
        ):
            logging.info("Skip file %s which already exists", file)
            continue

        # parse file to xarray
        ds = parser(file)

        # Rename variables
        ds = ds.rename(config["rename_variables"])

        # Merge Metadata
        merge_steps = {
            key: value
            for key, value in config.items()
            if key in ("merge", "merge_asof")
        }

        # for merge_type, inputs in merge_steps.items():
        #     if merge_type == 'merge':
        #         ds = ds.merge(config[])
        ## By SerialNumber

        # IOOS QC

        # Manual QC

        # Aggregate flags

        # Run Sentry warnings

        # Save to
        if config.get("file_output"):
            output_path = config["file_output"].get("path", file + ".nc").copy()
            if "$$" in output_path:
                path_variables = re.search(r"$$([^$]+)$$", output_path)
                for path_variable in path_variables:
                    if path_variable.startswith("global:"):
                        value = ds.attrs[path_variable[6:]]
                    elif path_variable in ds:
                        value = ds[path_variable].values

                    output_path = output_path.replace("$${path_variable}$$", value)
            else:
                output_path = config["file_output"]
            output_basename = os.path.basename(output_path)
            if not os.path.exists(output_basename):
                os.makedirs(output_basename)

            ds.to_netcdf(output_path)

        if config.get("upload_to_database"):
            # TODO update to database
            ds.to_dataframe().to_sql()
            pass


if __name__ == "__main__":
    with open(
        "/Users/jessybarrette/repo/ocean-data-parser-start/ocean_data_parser/sample-batch-config.yaml"
    ) as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
    convert(config)
