import os
import shutil
import sys
from glob import glob
from multiprocessing import Pool
from pathlib import Path

import click
import pandas as pd
from loguru import logger
from tqdm import tqdm
from xarray import Dataset

from ocean_data_parser import PARSERS, geo, process, read
from ocean_data_parser._version import __version__
from ocean_data_parser.batch.config import load_config
from ocean_data_parser.batch.registry import FileConversionRegistry
from ocean_data_parser.batch.utils import VariableLevelLogger, generate_output_path
from ocean_data_parser.parsers import utils

MODULE_PATH = Path(__file__).parent
DEFAULT_CONFIG_PATH = MODULE_PATH / "default-batch-config.yaml"


def save_new_config(ctx, _, path):
    if not path or ctx.resilient_parsing:
        return
    path = Path(path)
    if path.exists():
        # Do not overwrite an already existing file
        ctx.exit("Configuration file already exists!")

    logger.info(
        "Copy a default config to given path {} to {}",
        DEFAULT_CONFIG_PATH,
        path,
    )
    if not path.parent.exists():
        logger.info("Generate new directory")
        path.parent.mkdir(parents=True)
    shutil.copy(DEFAULT_CONFIG_PATH, path)
    ctx.exit()


def get_parser_list_string():
    bullets = "\n\t- "
    new_line = "\n"
    return (
        f"ocean-data-parser.parsers [{__version__}]{new_line}"
        f"{bullets}{bullets.join(PARSERS)} {new_line}"
    )


def validate_parser(ctx, _, value):
    """Test if given parser is available within parser list"""
    if value in PARSERS or value is None:
        return value
    raise click.BadParameter(
        click.style(
            f"parser should match one of the following options: {get_parser_list_string()}",
            fg="bright_red",
        )
    )


def get_parser_list(ctx, _, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(get_parser_list_string())
    ctx.exit()


@click.command(context_settings={"auto_envvar_prefix": "ODPY_CONVERT"})
@click.option(
    "-i",
    "--input-path",
    type=str,
    help="Input path to file list. It can be a glob expression (ex: *.cnv)",
)
@click.option(
    "--exclude",
    type=str,
    help="Glob expression of files to exclude.",
)
@click.option(
    "--parser",
    type=str,
    help=(
        "Parser used to parse the data. Default to auto detectection."
        " Use --parser_list to retrieve list of parsers available"
    ),
    callback=validate_parser,
)
@click.option(
    "--overwrite",
    type=bool,
    help="Overwrite already converted files when source file is changed.",
)
@click.option(
    "--multiprocessing",
    type=int,
    help=(
        "Run conversion in parallel on N processors."
        " None == all processors available"
    ),
)
@click.option(
    "-e",
    "--errors",
    type=click.Choice(["ignore", "raise"]),
    help="Error hanlding method",
)
@click.option(
    "--registry-path",
    type=click.Path(),
    help=(
        "File conversion registry path (*.csv or *.parquet)."
        " If --registry_path=None, no registry is used."
    ),
)
@click.option(
    "--output-path",
    type=click.Path(),
    help="Output directory where to save converted files.",
)
@click.option(
    "--output-file-name",
    type=click.Path(),
    help="Output file path where to save converted files.",
)
@click.option(
    "--output-file-suffix", type=click.Path(), help="Output file name suffix to add"
)
@click.option(
    "--config", "-c", type=click.Path(exists=True), help="Path to configuration file"
)
@click.option(
    "--new-config",
    is_eager=True,
    callback=save_new_config,
    type=click.Path(exists=False),
    help="Generate a new configuration file at the given path",
)
@click.option(
    "--parser-list",
    is_eager=True,
    is_flag=True,
    callback=get_parser_list,
    help="Get the list of parsers available",
)
@click.option(
    "--show-arguments",
    is_flag=False,
    flag_value="True",
    type=click.Choice(["stop", "True"]),
    default=None,
    help="Print present argument values. If  stop argument is given, do not run the conversion.",
)
@click.version_option(version=__version__, package_name="ocean-data-parser.convert")
def convert(**kwargs):
    """Run ocean-data-parser conversion on given files."""
    # Drop empty kwargs
    if kwargs.get("show_arguments"):
        click.echo("odpy convert parameter inputs:")
        click.echo("\n".join([f"{key}={value}" for key, value in kwargs.items()]))
        if kwargs["show_arguments"] == "stop":
            return
    kwargs.pop("show_arguments", None)

    kwargs = {
        key: None if value == "None" else value
        for key, value in kwargs.items()
        if value
    }

    BatchConversion(**kwargs).run()


class BatchConversion:
    def __init__(self, config=None, **kwargs):
        self.config = self._get_config(config, **kwargs)
        self.registry = FileConversionRegistry(**self.config["registry"])

    @staticmethod
    def _get_config(config: dict = None, **kwargs) -> dict:
        """Combine configuration dictionary and key arguments passed

        Args:
            config (dict, optional): Batch configuration. Defaults to None.

        Returns:
            dict: combined configuration
        """
        logger.info("Load configuration={}, kwargs={}", config, kwargs)
        output_kwarg = {
            key[7:]: kwargs.pop(key)
            for key in list(kwargs.keys())
            if key.startswith("output_")
        }
        registry_kwarg = {
            key[9:]: kwargs.pop(key)
            for key in list(kwargs.keys())
            if key.startswith("registry_")
        }
        config = {
            **load_config(DEFAULT_CONFIG_PATH),
            **(load_config(config) if isinstance(config, str) else config or {}),
            **kwargs,
        }
        config["output"].update(output_kwarg)
        config["registry"].update(registry_kwarg)
        return config

    def get_excluded_files(self) -> list:
        return (
            glob(self.config["exclude"], recursive=True)
            if self.config.get("exclude")
            else []
        )

    def get_source_files(self) -> list:
        excluded_files = self.get_excluded_files()
        return [
            file
            for file in glob(self.config["input_path"], recursive=True)
            if file not in excluded_files
        ]

    def _get_parser(self):
        logger.info("Load parser={}", self.config.get("parser", "None"))
        if not self.config.get("parser"):
            return None
        return read.load_parser(self.config["parser"])

    def _convert(self, files: list) -> list:
        # Load parser and generate inputs to conversion scripts
        parser = self._get_parser()
        inputs = ((file, parser, self.config) for file in files)
        tqdm_parameters = dict(unit="file", total=len(files))

        # single pool processing
        if "multiprocessing" not in self.config or self.config["multiprocessing"] in (
            False,
            1,
        ):
            return [
                _convert_file(input)
                for input in tqdm(inputs, **tqdm_parameters, desc="Run conversion")
            ]
        n_workers = self.config["multiprocessing"]
        n_workers = None if n_workers in ("True", True, "all") else n_workers
        with Pool(n_workers) as pool:
            return list(
                tqdm(
                    pool.imap(_convert_file, inputs),
                    **tqdm_parameters,
                    desc=(f"Run conversion with {n_workers or os.cpu_count()} workers"),
                )
            )

    def run(self):
        """Run Batch conversion"""
        logger.info("Run ocean-data-parser[{}] batch conversion", __version__)
        files = self.get_source_files()
        if not files:
            error_message = f"ERROR No files detected with {self.config['input_path']}"
            logger.error(error_message)
            sys.exit(error_message)

        self.registry.add(files)
        files = self.registry.get_modified_source_files()
        if not files:
            logger.info("No file to parse. Conversion completed")
            return self.registry
        logger.info(
            "{}/{} files needs to be converted", len(files), len(self.registry.data)
        )
        conversion_log = self._convert(files)
        conversion_log = (
            pd.DataFrame(
                conversion_log,
                columns=["sources", "output_path", "error_message", "warnings"],
            )
            .set_index("sources")
            .replace({"": None})
        )
        self.registry.update_fields(files, dataframe=conversion_log)
        self.registry.save()
        self.registry.summarize()
        logger.info("Conversion completed")
        return self.registry


def _convert_file(args):
    """Run file conversion while adding logging context

    Args:
        args (tuple): tuple [input file path, parser and configuration]

    Raises:
        error: If config['errors']['raise'] raise error encountered during processing

    Returns:
        tuple: input_path, output_path, error_message
    """
    with logger.contextualize(source_file=args[0]):
        warnings, errors = VariableLevelLogger("WARNING"), VariableLevelLogger("ERROR")
        output_file = None
        with logger.catch(reraise=args[2].get("errors") == "raise"):
            output_file = convert_file(args[0], args[1], args[2])
        output = (args[0], output_file, errors.values(), warnings.values())
        warnings.close()
        errors.close()
        return output


def convert_file(file: str, parser: str, config: dict) -> str:
    """Parse file with given parser and configuration

    Args:
        file (str): file path
        parser (str): ocean_data_parser.parsers parser.
        config (dict): Configuration use to apply the conversion

    Returns:
        str: output_path where converted file is saved
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
    ds = read.file(file, parser=parser)
    if not isinstance(ds, Dataset):
        raise RuntimeError(
            f"{parser.__module__}{parser.__name__}:{file} "
            "didn't return an Xarray Dataset"
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
    output_path = generate_output_path(ds, **config["output"])
    if not output_path.parent.exists():
        logger.info("Create new directory: {}", output_path.parent)
        output_path.parent.mkdir(parents=True)
    logger.trace("Save to: {}", output_path)
    ds.to_netcdf(output_path)

    return output_path


if __name__ == "__main__":
    convert(auto_envvar_prefix="ODPY_CONVERT")
