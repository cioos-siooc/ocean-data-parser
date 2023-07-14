import copy
import hashlib
import logging
import re
from pathlib import Path
from typing import Union

import pandas as pd

logger = logging.getLogger(__name__)

EMPTY_FILE_REGISTRY = pd.DataFrame(
    columns=["source", "last_update", "hash", "error_message", "output_path"]
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
            logger.warning("Unknown registry type")
            self.data = pd.DataFrame()

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

    def _get_hash(self, file):
        if isinstance(file, pd.Series):
            file = file.name

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
        if isinstance(source, pd.Series):
            source = source.name

        source = Path(source)
        return source.stat().st_mtime if source.exists() else None

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

        self.data = (
            pd.concat(
                [
                    self.data,
                    pd.DataFrame(
                        [
                            {
                                "source": source,
                                "last_update": self._get_mtime(source),
                                "hash": self._get_hash(source),
                            }
                            for source in sources
                        ]
                    ).set_index(["source"]),
                ],
            )
            .groupby(level=0)
            .last()
        )

    def _get_sources(self, sources: list):
        if not sources:
            return self.data.index.to_list()
        elif isinstance(sources, list):
            return sources
        return [sources]

    def update(self, sources: list = None):
        """Update registry hash and last_update attributes

        Args:
            sources (list, optional): Subset of file sources to update.
              Defaults to all entries.
        """
        sources = self._get_sources(sources)
        self.data.loc[sources, "hash"] = self.data.loc[sources].apply(
            self._get_hash, axis="columns"
        )
        self.data.loc[sources, "last_update"] = self.data.loc[sources].apply(
            self._get_mtime, axis="columns"
        )
        return

    def update_fields(self, sources=None, **kwargs):
        """Update given sources specific fields in attributes

        Args:
            sources (_type_): _description_
        """
        sources = self._get_sources(sources)
        for field, value in kwargs.items():
            if field not in self.data:
                self.data[field] = None
            self.data.loc[sources, field] = value

    def get_new_sources(self):
        return self.data[['error_message','output_path']].isna().all(axis=1)

    def get_modified_sources(self, sources: list = None):
        sources = self._get_sources(sources)
        if self.since:
            return self.get_sources_modified_since(sources)
        return self.get_sources_with_modified_hash(sources)

    def get_sources_with_modified_hash(self, sources: list = None) -> list:
        """Return list of source files with modified hash

        Args:
            sources (list, optional): Subset list of source files to review. Defaults to all.

        Returns:
            list: list of files with modified hash
        """
        sources = self._get_sources(sources)
        subset = self.data.loc[sources]
        is_different = (subset["hash"] != subset.index.to_series().apply(self._get_hash)) | self.data[['error_message','output_path']].isna().all(axis=1)
        return subset.loc[is_different].index.tolist()

    def get_sources_modified_since(
        self,
        since: Union[pd.Timedelta, pd.Timestamp, str] = None,
        sources=None,
    ):
        """Return list of modified source files since given timestamp or time interval.

        Args:
            sources (_type_, optional): subset of source files . Defaults to all.

        Returns:
            list: list of source files modified since given timestamp.
        """
        sources = self._get_sources(sources)
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
            since = pd.Timestamp.utcnow() - since

        logger.debug("Retrieve list of files modified since %s", since)
        subset = self.data.loc[self.data.index if not sources else sources]
        is_udpdated = (subset["last_update"] - since.timestamp() > 0) | self.data[['error_message','output_path']].isna().all(axis=1)
        return subset.loc[is_udpdated].index.tolist()

    def get_missing_sources(self):
        """Get list of missing sources

        Returns:
            list: missing sources
        """
        is_missing = ~self.data.index.to_series().map(Path).apply(Path.exists)
        return self.data.loc[is_missing].index.tolist()
