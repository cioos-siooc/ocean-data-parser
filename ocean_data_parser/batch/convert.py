import json
import os
import shutil
from glob import glob
from multiprocessing import Pool
from pathlib import Path

import click
import pandas as pd
from loguru import logger
from tqdm import tqdm
from xarray import Dataset

from ocean_data_parser import PARSERS, __version__, geo, read
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
    """Test if given parser is available within parser list."""
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


def validate_parser_kwargs(ctx, _, value):
    """Test if given parser_kwargs is a valid JSON string and return the parsed JSON object."""
    if not value:
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        raise click.BadParameter(
            click.style("parser-kwargs should be a valid JSON string", fg="bright_red")
        ) from None


@click.command(name="convert", context_settings={"auto_envvar_prefix": "ODPY_CONVERT"})
@click.option(
    "-i",
    "--input-path",
    type=str,
    help=(
        "Input path to file list. It can be a glob expression (ex: *.cnv)"
        " or a list of paths separated by a colons [:] (linux,mac) "
        "and semi-colons [;] (windows)."
    ),
)
@click.option(
    "--exclude",
    type=str,
    help="Glob expression of files to exclude.",
)
@click.option(
    "--parser",
    "-p",
    type=str,
    help=(
        "Parser used to parse the data. Default to auto detectection."
        " Use --parser_list to retrieve list of parsers available"
    ),
    callback=validate_parser,
)
@click.option(
    "--parser-kwargs",
    type=str,
    help=(
        "Parser key word arguments to pass to the parser. Expect a JSON string."
        ' (ex: \'{"globa_attributes": {"project": "test"}\')'
    ),
    default="{}",
    callback=validate_parser_kwargs,
)
@click.option(
    "--overwrite",
    type=bool,
    is_flag=True,
    default=False,
    help="Overwrite already converted files when source file is changed.",
)
@click.option(
    "--multiprocessing",
    type=int,
    help=(
        "Run conversion in parallel on N processors. None == all processors available"
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
    "-o",
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
def cli(**kwargs):
    """Ocean Data Parser Batch Conversion CLI Interface."""
    # Drop empty kwargs
    if kwargs.get("show_arguments"):
        click.echo("odpy convert parameter inputs:")
        click.echo("\n".join([f"{key}={value}" for key, value in kwargs.items()]))
        if kwargs["show_arguments"] == "stop":
            return
    kwargs.pop("show_arguments")
    kwargs.pop("new_config")
    convert(**kwargs)


def convert(**kwargs):
    """Run ocean-data-parser conversion on given files."""
    BatchConversion(**kwargs).run()


class BatchConversion:
    """Batch Conversion class to convert multiple files via a configuration file."""

    def __init__(self, config=None, **kwargs):
        """Create a batch conversion object.

        Args:
            config (dict, optional): Configuration use to apply
                the batch correction. Defaults to None.
            **kwargs: Key arguments passed to the class which
                overwrites the configuration file.
        """
        self.config = self._get_config(config, **kwargs)
        self.registry = FileConversionRegistry(**self.config.get("registry", {}))

    @staticmethod
    def _get_config(config: dict = None, **kwargs) -> dict:
        """Combine configuration dictionary and key arguments passed.

        Args:
            config (dict, optional): Batch configuration. Defaults to None.
            **kwargs: Key arguments passed to the function.

        Returns:
            dict: combined configuration
        """
        if config:
            logger.info("Load configuration file and ignore other inputs")
            return load_config(config) if isinstance(config, str) else config or {}

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
            **kwargs,
        }
        config["output"].update(output_kwarg)
        config["registry"].update(registry_kwarg)

        return config

    def get_excluded_files(self) -> list:
        return (
            [Path(file) for file in glob(self.config["exclude"], recursive=True)]
            if self.config.get("exclude")
            else []
        )

    def get_source_files(self) -> list:
        excluded_files = self.get_excluded_files()
        paths = self.config["input_path"]
        paths = paths.split(os.pathsep) if isinstance(paths, str) else paths

        return [
            Path(file)
            for path in paths
            for file in glob(path, recursive=True)
            if Path(file) not in excluded_files
        ]

    def load_input_table(self, table: dict) -> pd.DataFrame:
        """Load input table and apply pipe if needed."""
        if "path" not in table:
            raise ValueError("No path detected in input table")
        tables = []
        for item in glob(table["path"], recursive=True):
            if item.endswith(".csv"):
                df = pd.read_csv(item)
            else:
                raise ValueError("Only csv input table is supported")

            if table.get("add_table_name", False):
                df[table.get("table_name_column", "table_name")] = Path(item).stem

            tables.append(df)

        return pd.concat(tables, ignore_index=True)

    def get_source_files_from_input_table(self) -> ([], []):
        """Retrieve list of source files from input table. If input table is a dictionary, it will be loaded and processed."""
        input_table_config = self.config.get("input_table")
        if not input_table_config:
            logger.warning("No input table detected")
            return []

        if not isinstance(input_table_config, dict):
            raise ValueError("Input table should be a dictionary")

        # Load tables
        files_table = self.load_input_table(input_table_config)

        # Retrieve files
        search_files = (
            input_table_config.get("file_column_prefix", "")
            + files_table[input_table_config["file_column"]]
            + input_table_config.get("file_column_suffix", "")
        )
        files_table["files"] = search_files.apply(lambda x: glob(x, recursive=True))
        unmatched_glob = files_table["files"].apply(len) == 0
        if unmatched_glob.any():
            logger.warning(
                "No files detected with glob expression: {}",
                search_files[unmatched_glob].tolist(),
            )

        if input_table_config.get("exclude_columns"):
            files_table = files_table.drop(
                columns=input_table_config["exclude_columns"]
            )

        # Generate file list
        files_table = files_table.explode("files").dropna(subset="files")

        files = files_table["files"].apply(Path).tolist()
        if input_table_config.get("columns_as_attributes"):
            return (
                files,
                files_table.apply(lambda x: x.dropna().to_dict(), axis=1).tolist(),
            )
        # no attributes
        return files, [{}] * len(files)

    def _get_parser(self):
        logger.info("Load parser={}", self.config.get("parser", "None"))
        if not self.config.get("parser"):
            return None
        return read.import_parser(self.config["parser"])

    def _convert(self, inputs: list, n_files) -> list:
        # Load parser and generate inputs to conversion scripts
        tqdm_parameters = dict(unit="file", total=n_files)

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

    @logger.catch(reraise=True)
    def run(self):
        """Run Batch conversion."""
        logger.info(
            "Run ocean-data-parser[{}] convert {}",
            __version__,
            self.config.get("name", ""),
        )

        if self.config.get("input_path"):
            files = self.get_source_files()
            attributes = [{}] * len(files)
        elif self.config.get("input_table"):
            files, attributes = self.get_source_files_from_input_table()
        else:
            raise ValueError("No input path or input table detected")

        if not files:
            raise ValueError(f"No files detected with {self.config['input_path']}")

        self.registry.add(files)
        modified_files = self.registry.get_modified_source_files(
            overwrite=self.config["overwrite"]
        )
        if not modified_files:
            logger.info("No file to parse. Conversion completed")
            return self.registry

        # Retrieve attributes for modified files
        if attributes:
            modified_files_attrs = [
                attributes[files.index(file)] for file in modified_files
            ]
        else:
            modified_files_attrs = [
                {},
            ] * len(modified_files)

        logger.info(
            "{}/{} files needs to be converted", len(modified_files), len(files)
        )

        # Load parser
        parser = self._get_parser()

        # Generate inputs for conversion
        inputs = (
            (str(file), parser, self.config, attrs)
            for file, attrs in zip(modified_files, modified_files_attrs)
        )

        conversion_log = self._convert(inputs, n_files=len(modified_files))
        conversion_log = (
            pd.DataFrame(
                conversion_log,
                columns=["sources", "output_path", "error_message", "warnings"],
            )
            .set_index("sources")
            .replace({"": None})
        )
        conversion_log.index = conversion_log.index.map(Path)
        self.registry.update_fields(modified_files, dataframe=conversion_log)
        self.registry.save()
        self.registry.summarize(
            sources=modified_files, output=self.config.get("summary")
        )
        logger.info("Conversion completed")
        return self.registry


def _convert_file(args):
    """Run file conversion while adding logging context.

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
            output_file = convert_file(*args)
        output = (args[0], output_file, errors.values(), warnings.values())
        warnings.close()
        errors.close()
        return output


def convert_file(file: str, parser: str, config: dict, global_attributes=None) -> str:
    """Parse file with given parser and configuration.

    Args:
        file (str): file path
        parser (str): ocean_data_parser.parsers parser.
        config (dict): Configuration use to apply the conversion
        global_attributes (dict, optional): Global attributes to add to the dataset.

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
    ds = read.file(
        file,
        parser=parser,
        **(config.get("parser_kwargs") or {}),
        global_attributes=global_attributes,
    )
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
    for var, attrs in config.get("variable_attributes", {}).items():
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
        config.get("reference_stations", {}).get("path")
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
    for pipe in config.get("xarray_pipe", []):
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
        logger.debug("Create new directory: {}", output_path.parent)
        output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.trace("Save to: {}", output_path)
    ds.to_netcdf(output_path)

    return output_path


if __name__ == "__main__":
    cli(auto_envvar_prefix="ODPY_CONVERT")
