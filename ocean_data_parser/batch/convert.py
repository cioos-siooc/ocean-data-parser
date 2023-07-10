import logging.config
from glob import glob
from importlib import import_module
from pathlib import Path
from multiprocessing import Pool
import shutil

import click
import pandas as pd
from tqdm import tqdm
from xarray import Dataset

from ocean_data_parser import process
from ocean_data_parser.batch.config import load_config
from ocean_data_parser.batch.registry import FileConversionRegistry
from ocean_data_parser.batch.utils import generate_output_path
from ocean_data_parser.read import auto, utils

MODULE_PATH = Path(__file__).parent
DEFAULT_CONFIG_PATH = MODULE_PATH / "default-batch-config.yaml"

main_logger = logging.getLogger()
logger = logging.LoggerAdapter(main_logger, {"file": None})


def _get_paths(paths: str) -> list:
    if "*" in paths:
        path, glob_expr = paths.split("*", 1)
        glob_expr = f"*{glob_expr}"
        return [Path(path).glob(glob_expr)]
    return [Path(paths)]


@click.command()
@click.option(
    "--config", "-c", type=click.Path(exists=True), help="Path to configuration file"
)
@click.option(
    "--add",
    "-a",
    multiple=True,
    help="Extra parameters to include within the configuration",
)
@click.option(
    "--new_config",
    type=click.Path(exists=False),
    help="Generate a new configuration file at the given path",
)
def cli_files(config=None, add=None, new_config=None):
    if new_config:
        logger.info("Copy a default config to given path")
        shutil.copy(DEFAULT_CONFIG_PATH, new_config)
        return

    add = () if add is None else add
    added_inputs = dict((item.split("=", 1) for item in add))
    logger.info("Run config=%s", config)
    if add:
        logger.info("Modify configuration with added inputs=%s", added_inputs)
    main(config=config, **added_inputs)
    logger.info("Completed")


def main(config=None, **kwargs):
    """Ocean Data Parser batch conversion method

    Args:
        config (dict, optional): Configuration use to run the batch conversion.
            Defaults to None.
        **kwargs (optiona): Overwrite any configuration parameter by
            matching first level key.
    """

    def _convert_file(args):
        try:
            logger.extra["file"] = args[0]
            output_file = convert_file(args[0], args[1], args[2])
            file_registry.update(args[0])
            file_registry.update_fields(args[0], output_file=output_file)
        except Exception as error:
            if config.get("errors") == "raise":
                raise error
            logger.exception("Conversion failed", exc_info=True)
            file_registry.update_fields(args[0], error_message=error)

    # load config
    config = {
        **load_config(DEFAULT_CONFIG_PATH),
        **(load_config(config) if isinstance(config, str) else config or {}),
        **kwargs,
    }

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

    # Load config components
    if config.get("file_specific_attributes_path"):
        config["file_specific_attributes"] = pd.read_csv(
            config["file_specific_attributes_path"]
        ).set_index("file")

    if config.get("global_attribute_mapping").get("path"):
        config["globab_attribute_mapping"]["dataframe"] = pd.concat(
            [
                pd.read_csv(path)
                for path in _get_paths(config["global_attribute_mapping"]["path"])
            ]
        )
        missing_mapping_variables = [
            var not in config["globab_attribute_mapping"]["dataframe"]
            for var in config["globab_attribute_mapping"]["by_variables"]
        ]
        if any(missing_mapping_variables):
            raise KeyError(
                "Missing variables: %s from %s",
                config["globab_attribute_mapping"]["by_variables"][
                    missing_mapping_variables
                ],
                config["global_attribute_mapping"]["path"],
            )

    # Connect to database if given
    # TODO Establish connection to database

    # Load file registry
    file_registry = FileConversionRegistry(**config["registry"])

    # Get Files
    to_parse = []
    for input_path, parser in zip(
        config["input_path"].split(","), config["parser"].split(",")
    ):
        source_files = glob(input_path, recursive=config.get("recursive"))
        total_files = len(source_files)
        if not config.get("overwrite"):
            # Ignore files already parsed
            file_registry.add(source_files)
            source_files = file_registry.get_sources_with_modified_hash()
        to_parse += [
            {"files": source_files, "input_path": input_path, "parser": parser}
        ]
        logger.info("%s.%s will be parse", len(source_files), total_files)

    # Import parser module and load each files:
    for input in to_parse:
        parser = input["parser"]

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
        inputs = ((file, parser_func, config) for file in input["files"])
        tqdm_parameters = dict(
            desc="Run batch conversion", unit="file", total=len(input["files"])
        )
        if config.get("multiprocessing"):
            logger.debug("Run conversion in parallel with multiprocessing")
            with Pool(config["multiprocessing"]) as pool:
                tqdm(pool.imap(_convert_file, inputs), **tqdm_parameters)

        else:
            logger.debug("Run conversion ")
            for item in tqdm(inputs, **tqdm_parameters):
                _convert_file(item)

    file_registry.save()


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
    ds = parser(file)
    if not isinstance(ds, Dataset):
        raise RuntimeError(
            f"{parser.__module__}{parser.__name__}:{file} didn't return an Xarray Dataset"
        )

    # Update global and variable attributes from config
    ds.attrs.update({**config.get("global_attributes", {}), **_get_file_attributes()})
    for var, attrs in config.get("variable_attributes").items():
        if var in ds:
            ds[var].attrs.update(attrs)

    # Attribute Corrections
    ds.attrs.update(_get_mapped_global_attributes())

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
        overwrite = config["file_output"].pop("overwrite", False)
        output_path = generate_output_path(ds, source=file, **config["file_output"])
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


if __name__ == "__main__":
    cli_files()
