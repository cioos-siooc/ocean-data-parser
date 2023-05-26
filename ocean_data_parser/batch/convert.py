import logging.config
from glob import glob
from importlib import import_module
from pathlib import Path

from tqdm import tqdm
import click
from xarray import Dataset

from ocean_data_parser.batch.config import load_config
from ocean_data_parser.batch.registry import FileConversionRegistry
from ocean_data_parser.batch.utils import _generate_output_path
from ocean_data_parser.read import auto, utils

MODULE_PATH = Path(__file__).parent
DEFAULT_CONFIG_PATH = MODULE_PATH / "default-batch-config.yaml"

main_logger = logging.getLogger()
logger = logging.LoggerAdapter(main_logger, {"file": None})


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
def cli_files(config=None, add=None):
    add = () if add is None else add
    added_inputs = dict((item.split("=", 1) for item in add))
    logger.info("Run config=%s", config)
    if add:
        logger.info("Modify configuration with added inputs=%s", added_inputs)
    files(config=config, **added_inputs)
    logger.info("Completed")


def files(config=None, **kwargs):
    """Ocean Data Parser batch conversion method

    Args:
        config (dict, optional): Configuration use to run the batch conversion.
            Defaults to None.
        **kwargs (optiona): Overwrite any configuration parameter by
            matching first level key.
    """
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

    # Connect to database if given
    # TODO Establish connection to database

    # Load parse log file
    file_registry = FileConversionRegistry(path=config["file_registry"])

    # Get Files
    to_parse = []
    for input_path, parser in zip(
        config["input_path"].split(","), config["parser"].split(",")
    ):
        source_files = glob(input_path, recursive=config.get("recursive"))
        total_files = len(source_files)
        if not config.get("overwrite"):
            # Ignore files already parsed
            file_registry.add_missing(source_files)
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
        for file in tqdm(input["files"], desc="Run batch conversion", unit="file"):
            try:
                logger.extra[file] = file
                output_file = _file(file, parser_func, config)
                file_registry.update(file)
                file_registry.update_fields(file, output_file=output_file)
            except Exception as error:
                logger.exception("Conversion failed")
                file_registry.update_fields(file, error_message=error)

    file_registry.save()


def _file(file: str, parser: str, config: dict) -> str:
    """Parse file with given parser and configuration

    Args:
        file (str): file path
        parser (str): ocean_data_parser.read parser.
        config (dict): Configuration use to apply the conversion

    Returns:
        str: _description_
    """
    # parse file to xarray
    logger.extra["file"] = file
    ds = parser(file)
    if not isinstance(ds, Dataset):
        raise RuntimeError(
            f"{parser.__module__}{parser.__name__}:{file} didn't return an Xarray Dataset"
        )

    # Update global and variable attributes from config
    ds.attrs.update(config.get("global_attributes", {}))
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


if __name__ == "__main__":
    cli_files()
