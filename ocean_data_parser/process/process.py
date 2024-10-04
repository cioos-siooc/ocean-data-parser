import logging
from pathlib import Path
from typing import Callable, Union

import numpy as np
import pandas as pd
import xarray as xr

from ocean_data_parser.parsers import utils

logger = logging.getLogger(__name__)


@xr.register_dataset_accessor("process")
class Processing:
    def __init__(
        self,
        xarray_obj,
        lat="latitude",
        lon="longitude",
        time="time",
        z="depth",
        file_name_convention=None,
    ):
        self._obj = xarray_obj
        self.lat = lat
        self.lon = lon
        self.time = time
        self.z = z
        self.filename_convention = file_name_convention

    def to_netcdf(
        self,
        name: str = None,
        suffix: str = None,
        overwrite=True,
        time_variables_encoding=None,
        utc=True,
        **kwargs,
    ):
        name = Path(name or self.get_filename_from_convention(suffix=suffix))
        if name.suffix != ".nc":
            name = name.with_suffix(".nc")

        # Add directory if doesn't exists
        if not name.parent.exists():
            name.parent.mkdir()
        if not overwrite and name.exists():
            logger.warning("File already exists and won't be overwritten")
            return

        ds = self.standardize(
            time_variables_encoding=time_variables_encoding
            or utils.time_variables_default_encoding,
            utc=utc,
        )
        ds.to_netcdf(
            name or self.get_filename_from_convention(suffix=suffix) + ".nc", **kwargs
        )

    def get_filename_from_convention(self, suffix=None):
        return eval(f'f"{self.filename_convention}"', {}, {"ds": self._obj}) + (
            suffix or ""
        )

    def standardize(self, time_variables_encoding=None, utc=True):
        return utils.standardize_dataset(
            self._obj,
            time_variables_encoding=time_variables_encoding
            or utils.time_variables_default_encoding,
            utc=True,
        )

    def add_to_history(self, comment, timestamp=None):
        if timestamp is None:
            timestamp = pd.Timestamp.now(tz="UTC")

        if "history" not in self._obj.attrs:
            self._obj.attrs["history"] = "Batch Data Processing"

        self._obj.attrs["history"] = f"{timestamp.isoformat()} - {comment}"

    def correct_time_drift(
        self,
        initialization_time: pd.Timestamp,
        reference_time: pd.Timestamp,
        instrument_time: pd.Timestamp,
        time_variable: str = "time",
    ) -> xr.Dataset:
        time_drift = instrument_time - reference_time
        period = reference_time - initialization_time
        logger.info(
            "time drift: instrument_time - reference-time = %s seconds",
            time_drift.seconds(),
        )

        self._obj[time_variable] = self._obj[time_variable] + time_drift / period * (
            self._obj[time_variable] - initialization_time
        )
        self.add_to_history(
            f"Correct time drift: {initialization_time=}, {reference_time=}, {instrument_time=}"
            + f" => {time_drift.seconds()=}s over {period=}"
        )
        return self._obj

    def keep_deployment_period(
        self,
        depth_threshold: float = None,
        std_factor_threshold: float = 2,
        deployment_time_buffer: pd.Timedelta = pd.Timedelta(0),
        retrieval_time_buffer: pd.Timedelta = pd.Timedelta(0),
        depth: str = "depth",
        time: str = "time",
        dim=None,
        output: str = "crop",
        deployment_flag="deployement_flag",
    ) -> xr.Dataset:
        # Retrieve variables
        depth = self._obj[depth]
        time = self._obj[time]
        depth_std = depth.std()
        depth_median = depth.median()

        # Detect records measured within the deployment
        is_deployed = (
            depth_threshold is None or depth > depth_threshold
        ) and depth > depth_median - std_factor_threshold * depth_std

        # Define start and end times
        deployment_start = time.where(is_deployed, drop=True).min(dim=dim)
        deployment_end = time.where(is_deployed, drop=True).max(dim=dim)

        # Add time buffer
        try:
            deployment_start = deployment_start + deployment_time_buffer
            deployment_end = deployment_end + retrieval_time_buffer
        except TypeError:
            logger.warning(
                "Buffer periods are only compatible with datetime time variables"
            )

        if output == "crop":
            self._obj = self._obj.where(
                (deployment_start <= time) & (time <= deployment_end), drop=True
            )
            self.add_to_history("Crop time series include only deployment data")
        elif output == "flag":
            self._obj[deployment_flag] = (
                is_deployed.dims,
                is_deployed.values,
                {"long_name": "In Deployment"},
            )
            self.add_to_history(f"Generate a {deployment_flag} variable")

        return self._obj

    def gsw(
        self,
        func: str,
        gsw_args: tuple,
        name: str = None,
        extra_attrs: dict = None,
        ufunc: Callable[[xr.DataArray], xr.DataArray] = None,
        gsw_kwargs: dict = None,
    ) -> xr.DataArray:
        """Apply Gibbs Sea Water Tool Function (TEOS-10) to dataset.

        A full list of the TEOS-10 equations is available here:
        https://www.teos-10.org/pubs/gsw/html/gsw_contents.html

        This tool relies on the python
            - gsw python implementation: https://github.com/TEOS-10/GSW-python
            - gsw-xarray package: https://github.com/DocOtak/gsw-xarray

        Args:
            func (str): gsw function to apply
            name (str): output variable name
            gsw_args (tuple): tuple of the arguments to pass to the
                gsw function
            extra_attrs (dict, optional): Extra attributes to append to new variable.
                Defaults to None.
            ufunc (Callable[[xr.DataArray], xr.DataArray], optional):
                Function to apply the gsw function output (ex: lambda x:-1*x)
                Defaults to None.
            gsw_kwargs (dict, optional): keyword arguments to pass to the
                gsw function. Defaults to None.

        Returns:
            xr.DataArray: New
        """
        try:
            import gsw_xarray as gsw
        except ImportError:
            raise RuntimeError("Optional package gsw_xarray is required.")

        def _get_arg(arg):
            if arg in self._obj or arg in self._obj.coords:
                return self._obj[arg]
            return arg

        if not hasattr(gsw, func):
            raise KeyError("Function '{func}' is not available within the gsw package.")
        func = getattr(gsw, func)
        gsw_args = [_get_arg(arg) for arg in gsw_args]
        gsw_kwargs = {key: _get_arg(value) for key, value in (gsw_kwargs or {}).items()}
        data = func(*gsw_args, **gsw_kwargs)
        if ufunc:
            data = xr.apply_ufunc(ufunc, data, keep_attrs=True)
        data.attrs.update(extra_attrs or {})
        # add new variable
        if not (name or data.attrs["standard_name"]):
            raise NameError("Unknown variable name")

        self._obj[name or data.attrs["standard_name"]] = data
        self.add_to_history(
            f"Generate variable: {name}=gsw.{func}({gsw_args},{gsw_kwargs})"
        )
        return self._obj

    def qartod(
        self, config: Union[dict, str], agg: Union[dict, str] = "all"
    ) -> xr.Dataset:
        """Run ioos_qc tests on datasets

        Args:
            config (dict,str): Path to a file (*.yaml or *.json) or
               dictionray a ioos_qc configuration.
            agg (Union, optional): Aggregate the different QARTOD tests
                into distinct new variables.
                    - agg='all', a new variable will be generated for every single tests. Defaults to 'all'.
                    - agg = {
                        "output_variable_name": {
                            "tests": [
                                    #tuple of ioos_qc tests
                                    (stream,module,test)
                                ]
                            },
                            "streams": [list of streams to which applies this aggregated flag]
                        }

        Returns:
            xr.Dataset: New dataset with the added aggregatd flags.
        """
        try:
            from ioos_qc.config import Config
            from ioos_qc.qartod import QartodFlags, qartod_compare
            from ioos_qc.results import collect_results
            from ioos_qc.streams import XarrayStream
        except ImportError:
            raise RuntimeError(
                "Optional package ioos_qc is required: run `pip install ioos_qc`"
            )

        QARTOD_FLAGS = {
            name: value
            for name, value in QartodFlags.__dict__.items()
            if not name.startswith("__")
        }
        QARTOD_ATTRIBUTES = {
            "flag_meaning": " ".join(QARTOD_FLAGS.keys()),
            "flag_value": list(QARTOD_FLAGS.values()),
        }

        def _get_test_result(var, module, test):
            """Retrieve a specific test from ioos_qc collection
            of results in dict format
            """
            return results[var][module][test]

        def _get_aggregated_flag(tests: list) -> xr.DataArray:
            """Aggregate multiple QARTOD tests

            Args:
                tests (list): List of same size numpy.arrays
                    containing QARTOD flags

            Returns:
                xarray.DataArray: Aggregated QARTOD Flag DataArray
            """
            return (
                self._obj[tests[0][0]].dims,
                qartod_compare([_get_test_result(*test) for test in tests]),
                {
                    "long_name": "Aggregated Flag",
                    "standard_name": "aggregate_quality_flag",
                    **QARTOD_ATTRIBUTES,
                    "ioos_qc_tests": ";".join("-".join(test) for test in tests),
                },
            )

        def _add_ancillary(ancillary, variables):
            """Append too variable ancillary_variables attribute

            Args:
                ancillary (str): Ancillary variable, generally a flag variable.
                variable (list,str): Variables to which to append to ancillary_variables
            """
            variables = (
                variables.split(" ") if isinstance(variables, str) else variables
            )
            for variable in variables:
                self._obj[variable].attrs["ancillary_variables"] = (
                    self._obj[variable].attrs.get("ancillary_variables")
                    + " "
                    + ancillary
                    if self._obj[variable].attrs.get("ancillary_variables")
                    else ancillary
                )

        # Load ioos_qc config and run qc on xarray stream
        c = Config(config)
        qc = XarrayStream(
            self._obj, lon=self.lon, lat=self.lat, time=self.time, z=self.z
        )
        runner = qc.run(c)

        # Add flag variables to dataset
        if isinstance(agg, dict):
            results = collect_results(runner, how="dict")
            for ancillary, agg in agg.items():
                self._obj[ancillary] = _get_aggregated_flag(agg["tests"])
                _add_ancillary(ancillary, agg["streams"])
                self.add_to_history(
                    f"Generate QARTOD Aggregated Tests variable: '{agg}' which applies to {agg['streams']}"
                )

        elif agg == "all":
            results = collect_results(runner, how="list")
            for result in results:
                ancillary = f"{result.stream_id}_{result.package}_{result.test}"
                self._obj[ancillary] = (
                    self._obj[result.stream_id].dims,
                    result.results,
                    {**result.function.__dict__, **QARTOD_ATTRIBUTES},
                )
                _add_ancillary(ancillary, result.stream_id)
                self.add_to_history(
                    f"Generate QARTOD Test variable '{ancillary}' which applies to {result.stream_id}"
                )
        else:
            raise RuntimeError(f"Unknown test aggregator method: {agg}")

        return self._obj

    def drop_flagged_data(
        self,
        variables: str = None,
        flags: str = None,
        replace_by: Union[int, float, str] = np.nan,
        drop_flags=False,
    ) -> xr.Dataset:
        drop_flag_vars = []
        for variable in variables or self._obj:
            if "ancillary_variables" not in self._obj[variable].attrs:
                # if no ancillary_variables associated ignore
                continue

            # Get ancillary_variables list
            flag_variables = self._obj[variable].attrs["ancillary_variables"].split(" ")
            for flag_variable in flag_variables:
                self._obj[variable] = self._obj[variable].where(
                    ~self._obj[flag_variable].isin(flags), replace_by
                )

            if drop_flags:
                drop_flag_vars += flag_variables
                self._obj[variable].attrs.pop("ancillary_variables")

        if drop_flags:
            self._obj = self._obj.drop_vars(set(drop_flag_vars))
        return self._obj
