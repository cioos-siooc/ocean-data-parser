import logging
from datetime import datetime, timezone

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)

FLAG_LONG_NAME_PREFIX = "Quality_Flag: "
FLAG_DTYPE = "int32"
FLAG_CONVENTION = {
    "default": {
        "standard_name": "status_flag",
        "coverage_content_type": "qualityInformation",
        "ioos_category": "Quality",
        "flag_values": np.array([0, 1, 2, 3, 4, 5, 9]).astype(FLAG_DTYPE),
        "flag_meanings": " ".join(
            (
                "not_evaluated",
                "correct",
                "inconsistent_with_other_values",
                "doubtful",
                "erroneous",
                "modified",
                "missing",
            )
        ),
    },
    "QCFF_01": {
        "standard_name": "status_flag",
        "coverage_content_type": "qualityInformation",
        "ioos_category": "Quality",
        "flag_values": np.array([0, 1]).astype(FLAG_DTYPE),
        "flag_meanings": "undefined undefined",
    },
    "FFFF_01": {
        "standard_name": "status_flag",
        "coverage_content_type": "qualityInformation",
        "ioos_category": "Quality",
        "flag_values": np.array([0, 1]).astype(FLAG_DTYPE),
        "flag_meanings": "undefined undefined",
    },
}


def history_input(comment, date=datetime.now(timezone.utc)):
    """Genereate a CF standard history line: Timstamp comment."""
    return f"{date.strftime('%Y-%m-%dT%H:%M:%SZ')} {comment}\n"


def rename_qqqq_flags(dataset: xr.Dataset) -> xr.Dataset:
    """Convert QQQQ flags to Q{GF3} flag convention."""
    variables = list(dataset.variables)

    # Rename QQQQ flag convention
    qqqq_flags = {
        var: f"Q{variables[id-1]}"
        for id, var in enumerate(variables)
        if var.startswith("QQQQ")
    }
    if qqqq_flags:
        dataset = dataset.rename(qqqq_flags)
        dataset.attrs["history"] += history_input(
            f"Rename QQQQ flags to QXXXX convention: {qqqq_flags}",
        )
    return dataset


def add_flag_attributes(dataset):
    """odf_flag_variables handle the different conventions used within the ODF files
    over the years and map them to the CF standards.
    """

    def _add_ancillary(ancillary, variable):
        dataset[variable].attrs["ancillary_variables"] = (
            f"{dataset[variable].attrs.get('ancillary_variables','')} {ancillary}".strip()
        )
        return dataset[variable]

    # Add ancillary_variable attribute
    for variable in dataset.variables:
        if variable.startswith(("QCFF", "FFFF")):
            # add QCFF and FFFF as ancillary variables
            # to all non flag variables
            for var in dataset.variables:
                if not var.startswith("Q"):
                    _add_ancillary(variable, var)
        elif variable.startswith("Q") and variable[1:] in dataset:
            dataset[variable[1:]] = _add_ancillary(variable, variable[1:])
            dataset[variable].attrs["long_name"] = (
                f"Quality Flag for Parameter: {dataset[variable[1:]].attrs['long_name']}"
            )
        else:
            # ignore normal variables
            continue

        # Add flag convention attributes
        dataset[variable] = dataset[variable].astype(FLAG_DTYPE)
        dataset[variable].attrs.update(
            FLAG_CONVENTION.get(variable, FLAG_CONVENTION["default"])
        )
        dataset[variable].attrs.pop("units", None)
    return dataset


def fix_flag_variables(dataset: xr.Dataset) -> xr.Dataset:
    """Fix different issues related to flag variables within the ODFs."""

    def _replace_flag(dataset, flag_var, rename=None):
        if flag_var not in dataset:
            return dataset

        # Find related variables to this flag
        related_variables = [
            var
            for var in dataset.variables
            if flag_var in dataset[var].attrs.get("ancillary_variables", "")
        ]

        # Update long_name if flag is related to only one variable
        if len(related_variables) == 1:
            dataset[flag_var].attrs["long_name"] = (
                FLAG_LONG_NAME_PREFIX + dataset[related_variables[0]].attrs["long_name"]
            )

        # If no rename and affects only one variable. Name it Q{related_variable}
        if rename is None:
            # Ignore the original flag name
            related_variables_ = [
                var for var in related_variables if f"Q{var}" != flag_var
            ]
            if len(related_variables_) > 1:
                logger.error(
                    "Multiple variables are affected by %s, I'm not sure how to rename it.",
                    flag_var,
                )
            rename = f"Q{related_variables_[0]}"

        # Rename or drop flag variable
        if rename not in dataset:
            dataset = dataset.rename({flag_var: rename})
        elif (
            rename in dataset
            and (dataset[flag_var].values != dataset[rename].values).any()
        ):
            logger.error(
                "%s is different than %s flag. I'm not sure which one is the right one.",
                flag_var,
                rename,
            )
        elif (
            rename in dataset
            and (dataset[flag_var].values == dataset[rename].values).all()
        ):
            dataset = dataset.drop(flag_var)

        # Update ancillary_variables attribute
        for var in related_variables:
            dataset[var].attrs["ancillary_variables"] = (
                dataset[var].attrs["ancillary_variables"].replace(flag_var, rename)
            )
        return dataset

    #  List of problematic flags that need to be renamed
    temp_flag = {
        "QTE90_01": "QTEMP_01",
        "QTE90_02": "QTEMP_02",
        "QFLOR_01": None,
        "QFLOR_02": None,
        "QFLOR_03": None,
        "QCRAT_01": "QCNDC_01",
        "QCRAT_02": "QCNDC_02",
        "QTURB_01": None,
        "QWETECOBB_01": None,
        "QUNKN_01": None,
        "QUNKN_02": None,
    }
    for flag, rename in temp_flag.items():
        dataset = _replace_flag(dataset, flag, rename)

    return dataset
