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

    def _get_hash(self, file):

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
        return Path(file).exists() if isinstance(file,(str,Path)) else False

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
        new_data = pd.DataFrame({"source":sources})
        new_data['last_update'] = new_data['source'].apply(self._get_mtime)
        new_data['hash'] = new_data['source'].apply(self._get_hash)
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

    def _get_sources(self, sources: list):
        if not sources:
            return self.data.index.to_list()
        elif isinstance(sources, list):
            return sources
        return [sources]

    def _is_different_hash(self):
        return self.data["hash"] != self.data.index.map(self._get_hash)

    def _is_different_mtime(self):
        return self.data['last_update'] != self.data.index.map(self._get_mtime)
    
    def _is_modified_since(self):
        since = self._get_since_timestamp()
        return self.data.index.to_series().apply(self._get_mtime) - since >= 0
    
    def _source_exist(self):
        return self.data.index.to_series().apply(self._file_exists)

    def _output_file_exists(self):
        return self.data['output_path'].apply(self._file_exists)
    
    def _has_no_error(self):
        return self.data["error_message"].isna()
    
    def update(self, sources: list = None):
        """Update registry hash and last_update attributes

        Args:
            sources (list, optional): Subset of file sources to update.
              Defaults to all entries.
        """
        sources = self._get_sources(sources)
        self.data.loc[sources, "hash"] = list(map(self._get_hash, sources))
        self.data.loc[sources, "last_update"] = list(map(self._get_mtime, sources))
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
    
    def get_source_files_to_parse(self, overwrite=True):
        is_new = ~self._output_file_exists() & self._has_no_error()
        if not overwrite:
            return self.data.loc[is_new].index.to_list()

        if self.since:
            is_modified = self._is_modified_since()
        else:
            is_modified = self._is_different_hash()
        
        return self.data.loc[is_new | is_modified].index.to_list()

    def get_missing_sources(self):
        """Get list of missing sources

        Returns:
            list: missing sources
        """
        is_missing = ~self.data.index.to_series().map(Path).apply(Path.exists)
        return self.data.loc[is_missing].index.tolist()
