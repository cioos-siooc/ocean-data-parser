import copy
import hashlib
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

EMPTY_FILE_REGISTRY = pd.DataFrame(
    columns=["source", "last_update", "hash", "error_message", "output_path"]
).set_index("source")


class FileConversionRegistry:
    def __init__(
        self,
        path: str = "ocean_parser_file_registry.parquet",
        hashtype="sha256",
        block_size=65536,
        delta_time=0,
    ):
        self.path = Path(path)
        if self.path.exists():
            self.load()
        else:
            self.data = EMPTY_FILE_REGISTRY
        self.hashtype = hashtype
        self.hash_block_size = block_size
        self.delta_time = delta_time

    def load(self):
        """Load file registry if available otherwise return an empty dataframe"""
        if self.path is None or not self.path.exists():
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
        if not source.exists():
            return None
        return Path(source).stat().st_mtime

    def add_missing(self, sources: str):
        """Get add missing sources in file registry

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

    def update(self, sources: list = None):
        """Update registry with active files

        Args:
            sources (list, optional): Subset of file sources to update.
              Defaults to all entries.
            **kwargs: registry column and associate value to update within the file registry.
        """
        if not sources:
            sources = self.data.index.tolist()
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
        if not sources:
            sources = self.data.index.tolist()
        for field, value in kwargs.items():
            if field not in self.data:
                self.data[field] = None
            self.data.loc[sources, field] = value

    def get_sources_with_modified_hash(self, sources: list = None):
        if not sources:
            sources = self.data.index
        subset = self.data.loc[sources]
        is_different = subset["hash"] != subset.index.to_series().apply(self._get_hash)
        return subset.loc[is_different].index.tolist()

    def get_sources_with_mtime(self, sources=None, time_difference: float = 0):
        if not sources:
            sources = self.data.index
        subset = self.data.loc[sources]
        is_udpdated = (
            subset.index.to_series().apply(self._get_mtime) - subset["last_update"]
            > time_difference
        )
        return subset.loc[is_udpdated].index.tolist()

    def get_missing_sources(self):
        is_missing = ~self.data.index.to_series().map(Path).apply(Path.exists)
        return self.data.loc[is_missing].index.tolist()
