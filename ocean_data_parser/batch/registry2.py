from pathlib import Path
from loguru import logger
import pandas as pd
import hashlib
import subprocess
from typing import Union, List

REGISTRY_DTYPE = {
    "mtime": float,
    "size": int,
    "hash": str,
    "error_message": str,
    "output_path": str,
}


def _get_local_file_hash(path, hash_type: str = "md5", blocksize: int = 4096):
    hash_func = getattr(hashlib, hash_type)()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(blocksize), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


class Registry:
    """File registry for tracking files and their metadata"""

    def __init__(
        self,
        source: Path,
        destination: Path,
        include: List[str] = ["*"],
        exclude: List[str] = None,
        data: pd.DataFrame = None,
        registry_path: Path = "odpy-registry.csv",
        hash_type: str = "md5",
        blocksize: int = 4096,
    ):
        self.source = source
        self.destination = destination
        self.include = include or []
        self.exclude = exclude or []
        self.data = self.generate_registry(data)
        self.registry_path = registry_path
        self.hash_type = hash_type
        self.blocksize = blocksize

    @staticmethod
    def generate_registry(
        sources: list = None, data: Union[list, dict, pd.DataFrame] = None
    ) -> pd.DataFrame:
        """Generate a registry from the sources"""

        if data is None:
            df = pd.DataFrame(
                data={"source": sources},
                columns=list(REGISTRY_DTYPE.keys()) + ["source"],
            )
        else:
            df = pd.DataFrame(data=data)

        df = df.astype(
            {col: dtype for col, dtype in REGISTRY_DTYPE.items() if col in df}
        ).set_index("source")
        df.index = df.index.to_series().apply(Path)
        return df

    def load(self):
        """Load the registry from disk"""
        if not self.registry_path.exists():
            data = self.generate_registry()
        if self.registry_path.suffix == ".csv":
            data = pd.read_csv(self.registry_path, index_col="source")
        elif self.registry_path.suffix == ".parquet":
            data = pd.read_parquet(
                self.registry_path,
            )
        else:
            raise ValueError(f"Unknown registry file type: {self.registry_path.suffix}")
        data.index = data.index.to_series().apply(Path)
        self.data = data

    def save(self):
        """Save the registry to disk"""
        if self.registry_path.suffix == ".csv":
            self.data.to_csv(self.registry_path)
        elif self.registry_path.suffix == ".parquet":
            self.data.to_parquet(self.registry_path)
        else:
            raise ValueError(f"Unknown registry file type: {self.registry_path.suffix}")

    @staticmethod
    def _get_local_file_hash(
        file_path: Path, hash_type: str = "md5", blocksize: int = 4096
    ):
        """Get the hash of a local file"""
        return _get_local_file_hash(file_path, hash_type, blocksize)

    @staticmethod
    def _get_local_file_mtime(file_path: Path):
        """Get the modification time of a local file"""
        return file_path.stat().st_mtime

    @staticmethod
    def _get_local_file_size(file_path: Path):
        """Get the size of a local file"""
        return file_path.stat().st_size

    def _get_remote_file_via_rsync(
        self,
        file_paths: list = None,
        mtime: bool = True,
        size: bool = True,
        hash: bool = True,
        dry_run: bool = True,
        get_list: bool = True,
        file_list_temp_file: list = "file_lists.txt",
    ):
        """Get the modification time of a remote file via rsync"""
        # use rsync to get list of files and mofiication times
        options = []

        columns, out_format = ["source"], ["file=%n"]
        # handle optional arguments
        if mtime:
            columns.append("mtime_str")
            out_format.append("%M")
            options.append("--times")
        if size:
            columns.append("size")
            out_format.append("%l")
        if hash:
            columns.append("hash")
            out_format.append("%C")
            options.append("--checksum")
            options.append(f"--checksum-choice={self.hash_type}")
        if get_list:
            options.append(
                f'--out-format="{",".join(out_format)}"',
            )
        if dry_run:
            options.append("--dry-run")
        if file_paths:
            file_list = Path(file_list_temp_file)
            file_list.write_text("\n".join(file_paths))
            options.append(f"--files-from={file_list_temp_file}")

        rsync_command = [
            "rsync",
            *options,
            "-aPzv",
            *(f'--include="{self.source / include}"' for include in self.include),
            *(f'--exclude="{self.source / exclude}"' for exclude in self.exclude),
            f"{self.source}/",
            f"{self.destination}",
        ]

        logger.debug("Run command: %s", " ".join(rsync_command))
        result = subprocess.run(rsync_command, capture_output=True)
        if result.returncode != 0:
            raise RuntimeError(f"rsync failed: {result.stderr}")

        # decode stdout to string and split into lines and columns
        data = [
            dict(zip(columns, row[6:-1].split(",")))
            for row in result.stdout.decode("utf-8").split("\n")
            if row.startswith('"file=')
        ]
        data = self.generate_registry(data=data)

        # convert pandas timestamp[ns] to unix time
        data["mtime"] = (
            pd.to_datetime(data["mtime_str"], format="%Y/%m/%d-%H:%M:%S").astype(int)
            / 10**9
        )

        # filter out directories and extra columns
        data["is_dir"] = data.index.to_series().apply(lambda x: x.is_dir())
        data = data.query("is_dir == False")
        data = data.drop(columns=["mtime_str", "is_dir"])

        return data

    def get_source_files(
        self, source_files: list = None, get_mtime=True, get_size=True, get_hash=False
    ) -> pd.DataFrame:
        """Get a list of files from the source retrieve their:
        - modification times
        - size
        - hashes
        """
        if self.source.startswith("ssh://"):
            # use rsync to get list of files and mofiication times
            source_files = self._get_remote_file_via_rsync(
                self.source,
                self.destination,
                mtime=get_mtime,
                size=get_size,
                hash=get_hash,
            )
        else:
            # use local file system
            source_files = [
                {
                    "file_path": file,
                    "mtime": self._get_local_file_mtime(file) if get_mtime else None,
                    "size": self._get_local_file_size(file) if get_size else None,
                    "hash": self._get_local_file_hash(
                        file, self.hash_type, self.blocksize
                    )
                    if get_hash
                    else None,
                }
                for file in self.source.glob(self.search_for)
            ]

        return self.generate_registry(source_files)

    def compare(self, data):
        """Compare the data to the registry"""
        # check for new files
        new_files = data[~data.file_path.isin(self.data.index)]
        # check for modified files
        modified_files = data[
            data.file_path.isin(self.data.index)
            & (
                (data.mtime != self.data.mtime)
                | (data.size != self.data.size)
                | (data.hash != self.data.hash)
            )
        ]
        # check for deleted files
        deleted_files = self.data[~self.data.index.isin(data.file_path)]

        return new_files, modified_files, deleted_files

    def download_remote_source_files(
        self, new_files: list, download_file_list: Path = Path("download_file_list.txt")
    ) -> list:
        """Download new files from remote source"""

        # write list of new files to a temporary file
        download_file_list.write_text("\n".join(new_files))
        rsync_command = [
            "rsync",
            "-avz",
            "--files-from={tmp_file}",
            self.source.replace("ssh://", ""),
            self.destination,
        ]
        result = subprocess.run(rsync_command, capture_output=True, text=True)
        download_file_list.unlink()
        return result

    def update(
        self,
        new_files: pd.DataFrame = None,
        modified_files: pd.DataFrame = None,
        deleted_files: pd.DataFrame = None,
    ):
        """Update the registry"""
        # add new files
        self.data = self.data.append(new_files)
        # update modified files
        self.data.update(modified_files)
        # remove deleted files
        self.data.drop(deleted_files.index, inplace=True)

    def sync_registry(self):
        """Sync the registry with the source"""
        # get list of files from source
        data = self.get_source_files()
        # compare to registry
        new_files, modified_files, deleted_files = self.compare_to_registry(data)
        # update registry
        self.update_registry(new_files, modified_files, deleted_files)
        # save registry
        self.save()
        # return new files
        return new_files
