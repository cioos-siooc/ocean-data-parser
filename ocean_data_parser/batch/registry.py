import copy
import hashlib
import logging
from pathlib import Path
from typing import Union

import pandas as pd
from tqdm import tqdm

tqdm.pandas()
logger = logging.getLogger(__name__)

EMPTY_FILE_REGISTRY = pd.DataFrame(
    columns=["source", "mtime", "hash", "error_message", "output_path"]
).set_index("source")


REGISTRY_DTYPE = {
    "mtime": float,
    "hash": str,
    "error_message": str,
    "output_path": str,
}


def generate_registry(sources=None):
    return pd.DataFrame(
        data={"source": sources},
        columns=list(REGISTRY_DTYPE.keys()) + ["source"],
    ).set_index("source")


class FileConversionRegistry:
    def __init__(
        self,
        path: str = None,
        data: pd.DataFrame = generate_registry(),
        hashtype: str = "sha256",
        block_size: int = 65536,
    ):
        self.path = Path(path) if path else None
        self.data = data
        self.hashtype = hashtype
        self.hash_block_size = block_size

        if self.path and self.path.exists() and data.empty:
            self.load()

    def load(self, overwrite=False):
        """Load file registry if available otherwise return an empty dataframe"""

        def _as_path(path):
            return Path(path) if pd.notna(path) else path

        if not self.data.empty and not overwrite:
            logger.warning(
                "Registry already contains data and won't reload from: %s", self.data
            )
            return
        elif self.path is None or not self.path.exists():
            self.data = generate_registry()
        elif self.path.suffix == ".csv":
            self.data = pd.read_csv(self.path, index_col="source", dtype=REGISTRY_DTYPE)
        elif self.path.suffix == ".parquet":
            self.data = pd.read_parquet(self.path)
        else:
            raise TypeError("Unknown registry type")

        self.data.index = self.data.index.map(Path)
        self.data["output_path"] = self.data["output_path"].apply(_as_path)
        return self

    def save(self):
        """_summary_"""
        df = self.data.drop(columns=[col for col in self.data if col.endswith("_new")])
        if not self.path:
            return
        elif self.path.suffix == ".csv":
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

    @staticmethod
    def _get_mtime(source: str) -> float:
        """Get file modified time

        Args:
            source (str): source file path

        Returns:
            float: time in unix time
        """
        source = Path(source)
        return source.stat().st_mtime if source.exists() else None

    @staticmethod
    def _file_exists(file):
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
        new_data = generate_registry(sources)

        # Retrieve mtime and hash only if a registry is actually saved
        if self.path:
            logger.info("Get new files mtime")
            new_data = new_data.assign(
                mtime=new_data.index.map(self._get_mtime),
                hash=new_data.index.map(self._get_hash),
            )

        self.data = (
            new_data
            if self.data.empty
            else pd.concat(
                [
                    self.data,
                    new_data,
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

    def get_modified_source_files(self, overwrite: bool = True) -> list:
        """Return the list of files that needs to be parsed

        Args:
            overwrite (bool, optional): overwrite files already parsed
            and for which output already exists. Defaults to True.

        Returns:
            list: list of source files to parse
        """
        if not overwrite or not self.path:
            return self.data.loc[self._is_new_file()].index.to_list()

        if self.hashtype:
            is_modified = self._is_different_hash()
        else:
            is_modified = self._is_different_mtime()

        return self.data.loc[self._is_new_file() | is_modified].index.to_list()

    def get_missing_sources(self) -> list:
        """Get list of missing sources

        Returns:
            list: missing sources
        """
        is_missing = ~self.data.index.to_series().map(Path).apply(Path.exists)
        return self.data.loc[is_missing].index.tolist()

    def summarize(self, sources=None, by="error_message", output=None):
        """Generate a summary of the file registry errors"""
        if sources:
            data = self.data.loc[sources]
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
        if not errors.empty:
            logger.error("The following errors were captured:\n%s", errors)
            if output:
                errors.to_csv(output)
