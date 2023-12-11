from pathlib import Path
from loguru import logger
import pandas as pd
import hashlib
import subprocess
from typing import Union, List
from tqdm import tqdm
from copy import copy, deepcopy

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
        source: Union[Path, str],
        destination: Path,
        include: List[str] = ["*"],
        exclude: List[str] = None,
        registry_path: Union[Path, str] = "odpy-registry.csv",
        hash_type: str = "md5",
        blocksize: int = 4096,
    ):
        registry_path = Path(registry_path)
        if registry_path.exists():
            logger.debug("Loading registry from %s", registry_path)
        self.source = source
        self.destination = destination
        self.include = include or []
        self.exclude = exclude or []
        self.registry_path = registry_path
        self.hash_type = hash_type
        self.blocksize = blocksize

        if registry_path.exists():
            self.load()
        else:
            self.data = self.new()

    @staticmethod
    def new(
        files: list = None, data: Union[list, dict, pd.DataFrame] = None
    ) -> pd.DataFrame:
        """Generate a registry from the sources"""

        if data:
            df = pd.DataFrame(data=data)
        else:
            df = pd.DataFrame(
                data={"files": files},
                columns=list(REGISTRY_DTYPE.keys()) + ["files"],
            )

        df = df.astype(
            {col: dtype for col, dtype in REGISTRY_DTYPE.items() if col in df}
        ).set_index("files")
        df.index = df.index.to_series().apply(Path)
        return df

    def load(self):
        """Load the registry from disk"""
        if not self.registry_path.exists():
            logger.warning("Registry file does not exist: %s", self.registry_path)
            return

        if self.registry_path.suffix == ".csv":
            data = pd.read_csv(self.registry_path)
        elif self.registry_path.suffix == ".parquet":
            data = pd.read_parquet(
                self.registry_path,
            )
        else:
            raise ValueError(f"Unknown registry file type: {self.registry_path.suffix}")

        if "source" in data:
            logger.warning(
                "Registry file contains source column, which is deprecated and replaced by files"
            )
            data.rename(columns={"source": "files"}, inplace=True)

        data.set_index("files", inplace=True)
        data.index = data.index.to_series().apply(Path)
        self.data = data

    def save(self, output: Path = None):
        """Save the registry to disk"""
        output = output or self.registry_path
        if output.suffix == ".csv":
            self.data.to_csv(output)
        elif output.suffix == ".parquet":
            self.data.to_parquet(output)
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

    def _get_files_from_local_source(
        self, files: list, mtime: bool, size: bool, hash: bool
    ):
        file_properties = []
        if mtime:
            file_properties.append("mtime")
        if size:
            file_properties.append("size")
        if hash:
            file_properties.append("hash")

        file_list = []
        tqdm_items = {
            "desc": f"Get source files {file_properties}",
            "unit": "files",
        }
        if not files:
            files = [
                file
                for include in self.include
                for file in Path(self.source).glob(include)
            ]

        for file in files or tqdm(files, **tqdm_items):
            file_list += [
                {
                    "files": file,
                    "mtime": self._get_local_file_mtime(file) if mtime else None,
                    "size": self._get_local_file_size(file) if size else None,
                    "hash": self._get_local_file_hash(
                        file, self.hash_type, self.blocksize
                    )
                    if hash
                    else None,
                }
            ]
        return file_list

    def _get_files_from_remote_source_via_rsync(
        self,
        files: list = None,
        mtime: bool = True,
        size: bool = True,
        dry_run: bool = True,
        get_list: bool = True,
        file_list_temp_file: list = "file_lists.txt",
    ):
        """Get the modification time of a remote file via rsync"""
        # use rsync to get list of files and mofiication times
        options = []

        columns, out_format = ["files"], ["file=%n"]
        # handle optional arguments
        if mtime:
            columns.append("mtime_temp")
            out_format.append("%M")
            options.append("--times")
        if size:
            columns.append("size")
            out_format.append("%l")
        if get_list:
            options.append(
                f'--out-format="{",".join(out_format)}"',
            )
        if dry_run:
            options.append("--dry-run")
        if files:
            file_list = Path(file_list_temp_file)
            file_list.write_text("\n".join(files))
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
        process = subprocess.Popen(
            rsync_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={"TZ": "UTC"},
        )
        stdout, stderr = process.communicate()
        if not stderr != 0:
            raise RuntimeError(f"rsync failed: {stderr}")

        # decode stdout to string and split into lines and columns
        data = [
            dict(zip(columns, row[6:-1].split(",")))
            for row in stdout.split("\n")
            if row.startswith('"file=')
        ]
        data = self.new(data=data)

        # convert pandas timestamp[ns] to unix time
        if (
            "mtime_temp" in data
            and data["mtime_temp"]
            .str.match("\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}")
            .all()
        ):
            logger.warning(
                "rsync retrieved a timestamp for mtime instead of unix time. "
                "The timezone in this case is unknown and may be incorrect."
            )
            data["mtime"] = (
                pd.to_datetime(data["mtime_temp"], format="%Y/%m/%d-%H:%M:%S").astype(
                    int
                )
                / 10**9
            )

        # filter out directories and extra columns
        data["is_dir"] = data.index.to_series().apply(lambda x: x.is_dir())
        data = data.query("is_dir == False")
        data = data.drop(columns=["mtime_temp", "is_dir"])

        return data

    def _get_files_from_source(
        self, files: list = None, mtime=True, size=True, hash=False
    ) -> pd.DataFrame:
        """Get a list of files from the source retrieve their:
        - modification times
        - size
        - hashes
        """
        if str(self.source).startswith("ssh://"):
            if hash:
                raise NotImplementedError(
                    "Hashing of remote files is not implemented yet"
                )
            # use rsync to get list of files and mofiication times
            file_list = self._get_files_from_remote_source_via_rsync(
                files=files,
                mtime=mtime,
                size=size,
                hash=hash,
                dry_run=True,
                get_list=True,
            )
        else:
            # use local file system
            file_list = self._get_files_from_local_source(files, mtime, size, hash)
        return self.new(data=file_list)

    def get_files_from_source(
        self, files: list = None, mtime=True, size=True, hash=False
    ) -> pd.DataFrame:
        """Get a list of files from the source retrieve their modification times, size and hashes.
        Hashes are retrieved only if mtime and/or size is either different or unknown"""

        files = self._get_files_from_source(
            files=files, mtime=mtime, size=size, hash=False
        )
        if not hash:
            return files

        # get list of new files and modified files
        new_files = files[files.index.isin(self.data.index) == False].index
        modified_files = files[
            files.index.isin(self.data.index)
            & ((files.mtime != self.data.mtime) | (files.size != self.data.size))
        ].index
        unknown_hash_files = files[
            files.index.isin(self.data.index) & (files.hash.isna())
        ].index

        # get hashes for new files and modified files
        get_hash_from = files.loc[
            set(new_files + modified_files + unknown_hash_files)
        ].index.to_list()
        if get_hash_from:
            files.loc[get_hash_from, "hash"] = self._get_files_from_source(
                files=get_hash_from, mtime=False, size=False, hash=True
            ).hash

        return files

    def get_modified_files(self, file_list):
        """Compare file list versus the registry and return modified files"""
        return file_list[
            file_list.files.isin(self.data.index)
            & (
                (file_list.mtime != self.data.mtime)
                | (file_list.size != self.data.size)
                | (file_list.hash != self.data.hash)
            )
        ]

    def get_new_files(self, file_list):
        """Compare file list versus the registry and return new files"""
        return file_list[~file_list.files.isin(self.data.index)]

    def get_missing_files(self, file_list):
        """Compare file list versus the registry and return missing files"""
        return self.data[~self.data.index.isin(file_list.files)]

    def download_remote_source_files(
        self, files: list, download_file_list: Path = Path("download_file_list.txt")
    ) -> list:
        """Download new files from remote source"""

        # write list of new files to a temporary file
        download_file_list.write_text("\n".join(files))
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

    def add(self, files: pd.DataFrame):
        """Add new files to the registry"""
        # add new files
        self.data = self.data.append(files)

    def update(
        self,
        files: pd.DataFrame = None,
    ):
        """Update the registry"""
        self.data.update(files)

    def sync_registry(self):
        """Sync the registry with the source"""
        # get list of files from source
        data = self._get_files_from_source()
        # compare to registry
        new_files, modified_files, deleted_files = self.compare_to_registry(data)
        # update registry
        self.update_registry(new_files, modified_files, deleted_files)
        # save registry
        self.save()
        # return new files
        return new_files
