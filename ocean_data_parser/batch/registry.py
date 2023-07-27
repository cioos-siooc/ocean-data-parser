import copy
import hashlib
import logging
import re
from pathlib import Path
from typing import Union

import pandas as pd
from tqdm import tqdm

tqdm.pandas()
logger = logging.getLogger(__name__)

EMPTY_FILE_REGISTRY = pd.DataFrame(
    columns=["source", "mtime", "hash", "error_message", "output_path"]
).set_index("source")


class FileConversionRegistry:
    def __init__(
        self,
        path: str = "ocean_parser_file_registry.csv",
        data: pd.DataFrame = EMPTY_FILE_REGISTRY,
        hashtype: str = "sha256",
        block_size: int = 65536,
        since: Union[pd.Timestamp, pd.Timedelta, str] = None,
    ):
        self.path = Path(path)
        self.data = data
        self.hashtype = hashtype
        self.hash_block_size = block_size
        self.since = since

        if self.path.exists() and data.empty:
            self.load()

    def load(self, overwrite=False):
        """Load file registry if available otherwise return an empty dataframe"""
        if not self.data.empty and not overwrite:
            logger.warning(
                "Registry already contains data and won't reload from: %s", self.data
            )
            return
        elif self.path is None or not self.path.exists():
            self.data = pd.DataFrame()
        elif self.path.suffix == ".csv":
            self.data = pd.read_csv(self.path)
        elif self.path.suffix == ".parquet":
            self.data = pd.read_parquet(self.path)
        else:
            raise TypeError("Unknown registry type")

        if "source" in self.data:
            self.data = self.data.set_index(["source"])
        return self

    def save(self):
        """_summary_"""
        df = self.data.drop(columns=[col for col in self.data if col.endswith("_new")])
        if self.path.suffix == ".csv":
            df.to_csv(self.path)
        elif self.path.suffix == ".parquet":
            df.to_parquet(self.path)
        else:
            logger.error("Unknown registry format: %s", self.path)

    def copy(self):
        return copy.copy(self)

    def deepcopy(self):
        return copy.deepcopy(self)

    def _get_hash(self, file: Union[str, Path]) -> str:
        """Retriveve file hash

        Args:
            file (str, Path): path to file

        Returns:
            str: hash
        """
        file = Path(file)
        if not file.exists():
            return None
        file_hash = hashlib.new(self.hashtype)
        with open(file, "rb") as file_handle:
            file_block = file_handle.read(self.hash_block_size)
            while len(file_block) > 0:
                file_hash.update(file_block)
                file_block = file_handle.read(self.hash_block_size)
            return file_hash.hexdigest()

    def _get_mtime(self, source: str) -> float:
        """Get file modified time

        Args:
            source (str): source file path

        Returns:
            float: time in unix time
        """
        source = Path(source)
        return source.stat().st_mtime if source.exists() else None

    def _get_since_timestamp(self, since=None) -> pd.Timestamp:
        """Convert since attribute to a pd.Timestamp"""
        if not since:
            since = self.since
        if isinstance(since, str):
            # Detect string input type
            # - format "1231 ad" is likely a timedelta
            # - otherwise assume it's a date
            if re.fullmatch(r"[\d\.]+\s*\w+", since):
                since = pd.Timedelta(since)
            else:
                since = pd.Timestamp(since)

        # Convert timedelta to present time
        if isinstance(since, pd.Timedelta):
            return (pd.Timestamp.utcnow() - since).timestamp()
        elif isinstance(since, pd.Timestamp):
            return since.timestamp()
        return since

    def _file_exists(self, file):
        return Path(file).exists() if isinstance(file, (str, Path)) else False

    def add(self, sources: list):
        """Add add sources to file registry and ignore already known sources

        Args:
            sources (str): list of files to get parameters form

        Returns:
            DataFrame: Dataframe of the sources parameters.
        """
        sources = [source for source in sources if source not in self.data.index]
        if not sources:
            return
        new_data = pd.DataFrame({"source": sources})
        logger.info("Get new files mtime")
        new_data["mtime"] = new_data["source"].progress_apply(self._get_mtime)
        logger.info("Get new files hash")
        new_data["hash"] = new_data["source"].progress_apply(self._get_hash)
        self.data = (
            pd.concat(
                [
                    self.data,
                    new_data.set_index(["source"]),
                ],
            )
            .groupby(level=0)
            .last()
        )

    def _get_sources(self, sources: list) -> list:
        return sources if isinstance(sources, list) else self.data.index.to_list()

    def _is_different_hash(self):
        # Speed up hash difference by first filtering out data with unchanged mtime
        is_different = self._is_different_mtime()
        is_different.loc[is_different] = (
            self.data.loc[is_different].index.map(self._get_hash)
            != self.data.loc[is_different]["hash"]
        )
        return is_different

    def _is_different_mtime(self) -> pd.Series:
        return self.data["mtime"] != self.data.index.map(self._get_mtime)

    def _is_modified_since(self) -> pd.Series:
        if self.since is None:
            return pd.Series(False, self.data.index)
        since = self._get_since_timestamp()
        return self.data.index.to_series().apply(self._get_mtime) - since >= 0

    def _is_new_file(self) -> pd.Series:
        return ~self._output_file_exists() & self._has_no_error()

    def _source_exist(self) -> pd.Series:
        return self.data.index.to_series().apply(self._file_exists)

    def _output_file_exists(self) -> pd.Series:
        return self.data["output_path"].apply(self._file_exists)

    def _has_no_error(self) -> pd.Series:
        return self.data["error_message"].isna()

    def update(self, sources: list = None):
        """Update registry hash and mtime attributes

        Args:
            sources (list, optional): Subset of file sources to update.
              Defaults to all entries.
        """
        sources = self._get_sources(sources)
        self.data.loc[sources, "hash"] = list(map(self._get_hash, sources))
        self.data.loc[sources, "mtime"] = list(map(self._get_mtime, sources))

    def update_fields(
        self,
        sources: list = None,
        placeholder=None,
        dataframe: Union[list, pd.DataFrame] = None,
        **kwargs
    ):
        """Update registry sources with given values

        Args:
            sources (list): list of source files to update
            placeholder (optional): Placeholder to use when generating
                new variables. Defaults to None.
            dataframe (list, pd.DataFrame, optional): dataframe with source
                as index to update registry.
            **kwargs (optional): key argument list of values replace
                by within the registry with.

        Raises:
            Exception: _description_
        """
        if dataframe is not None and kwargs:
            raise ValueError(
                "Can't update fields with a mix of arguments " "and keyword arguments"
            )

        # If unique source is given convert it to a string
        if not isinstance(sources, list) and sources in self.data.index:
            sources = [sources]

        # Generate update dataframe
        if not isinstance(dataframe, pd.DataFrame):
            dataframe = pd.DataFrame(
                dataframe or kwargs,
                index=self.data.index if sources is None else sources,
            )

        # Add missing columns
        for col in dataframe.columns:
            if col not in self.data:
                self.data[col] = placeholder

        self.data.update(dataframe, overwrite=True)

    def get_source_files_to_parse(self, overwrite: bool = True) -> list:
        """Return the list of files that needs to be parsed

        Args:
            overwrite (bool, optional): overwrite files already parsed
            and for which output already exists. Defaults to True.

        Returns:
            list: list of source files to parse
        """
        if not overwrite:
            return self.data.loc[self._is_new_file()].index.to_list()

        if self.since:
            is_modified = self._is_modified_since()
        else:
            is_modified = self._is_different_hash()

        return self.data.loc[self._is_new_file() | is_modified].index.to_list()

    def get_missing_sources(self) -> list:
        """Get list of missing sources

        Returns:
            list: missing sources
        """
        is_missing = ~self.data.index.to_series().map(Path).apply(Path.exists)
        return self.data.loc[is_missing].index.tolist()

    def summarize(self, sources=None, by="error_message", output="error_report.csv"):
        """Generate a summary of the file registry errors"""
        if sources:
            data = self.data[self.data[sources]]
        else:
            data = self.data
        succeed = len(data.query("error_message.isna()"))
        logger.info("%s/%s sources were processed", succeed, len(data))
        errors = (
            data.dropna(subset="error_message")
            .astype({"error_message": str})
            .reset_index()
            .groupby(by)
            .agg({"source": ["count", list]})
        )
        errors.columns = (" ".join(col) for col in errors.columns)
        logger.info("The following errors were captured: %s", errors)
        if output:
            errors.to_csv(output)
