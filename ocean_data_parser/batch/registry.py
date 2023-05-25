import hashlib
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class FileConversionRegistry:
    def __init__(
        self,
        path: str = "ocean_parser_file_registry.parquet",
        hashtype="sha256",
        delta_time=None,
    ):
        self.path = Path(path)
        self.data = pd.DataFrame()
        self.hashtype = hashtype
        self.hash_block_size = 65536
        self.delta_time = delta_time

    def load(self):
        """Load file registry if available otherwise return an empty dataframe"""
        if self.path is None or not self.path.exists():
            self.data = pd.DataFrame()
        elif self.path.suffix == ".csv":
            self.data = pd.read_csv(self.path, index_col=["source"])
        elif self.path.suffix == ".parquet":
            self.data = pd.read_parquet(self.path, index_col=["source"])
        else:
            logger.warning("Unknown registry type")
            self.data = pd.DataFrame()
        return self

    def save(self):
        """_summary_"""
        self.data = self.data.drop(
            columns=[col for col in self.data if col.endswith("_new")]
        )
        if self.path.suffix == ".csv":
            self.data.to_csv(self.path)
        elif self.path.suffix == ".parquet":
            self.data.to_parquet(self.path)
        else:
            logger.error("Unknown registry format: %s", self.path)

    def _get_hash(self, file):
        file_hash = hashlib.new(self.hashtype)
        with open(file, "rb") as file_handle:
            file_block = file_handle.read(self.hash_block_size)
            while len(file_block) > 0:
                file_hash.update(file_block)
                file_block = file_handle.read(self.hash_block_size)
            return file_hash.hexdigest()

    def _get_modified_times(self, source: str) -> float:
        """Get file modified time

        Args:
            source (str): source file path

        Returns:
            float: time in unix time
        """
        return Path(source).stat().st_mtime

    def _get_sources_entry(self, sources: str, **kwargs):
        """Get source file parameters

        Args:
            sources (str): list of files to get parameters form

        Returns:
            DataFrame: Dataframe of the sources parameters.
        """
        return pd.DataFrame(
            [
                {
                    "source": source,
                    "last_update": self._get_modified_times(source),
                    "hash": self._get_hash(source),
                    "error_message": None,
                    **kwargs,
                }
                for source in sources
            ]
        ).set_index(["source"])

    def load_sources(self, sources: list):
        new_entry = self._get_sources_entry(sources)
        self.data = self.data.join(new_entry.add_suffix("_new"), how="outer")

    def get_modified_hashes(self) -> list:
        """Get source files list with modified hash different then registry 
        and not error associated.

        Returns:
            list: source files list
        """
        if "hash" not in self.data and "hash_new" in self.data:
            return self.data.index.to_list()
        return self.data.query(
            "hash != hash_new and error_message.isna()"
        ).index.to_list()

    def get_modified_times(self) -> list:
        """Get source files list with modified times different then registry 
        and not error associated.

        Returns:
            list: source file list
        """
        if "last_update" not in self.data and "last_update_new" in self.data:
            return self.data.index.to_list()
        return self.data.query(
            "last_update != last_update_new and error_message.isna()"
        ).index.to_list()

    def update_source(self, source):
        """Update a given source file registry hash and modified time.

        Args:
            source (str): source file in registry to update
        """
        for column in self.data.filter(regex="_new$"):
            self.data.loc[source, column[:-4]] = self.data.loc[source, column]

    def add_to_source(self, source, **kwargs):
        """Add update given key and value related to a source file in the registry"""
        for key, value in kwargs.items():
            if key not in self.data:
                self.data[key] = None
            self.data.loc[source, key] = value
