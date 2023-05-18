import logging
import pandas as pd
import numpy as np
import json
from pathlib import Path

import mkdocs.plugins

logger = logging.getLogger("mkdocs")


def quote_column(col):
    return "`" + col + "`"


def get_active_branch_name():

    head_dir = Path(".") / ".git" / "HEAD"
    with head_dir.open("r") as f:
        content = f.read().splitlines()

    for line in content:
        if line[0:4] == "ref:":
            return line.partition("refs/heads/")[2]


def get_odf_vocab_markdown(version):
    df = pd.read_csv(
        "ocean_data_parser/read/dfo/odf_source/references/reference_vocabulary.csv"
    )
    for column in [
        "accepted_units",
        "accepted_scale",
        "accepted_instruments",
        "apply_function",
    ]:
        df[column] = quote_column(df[column])
    with open(f"{version}/docs/read/dfo/odf-hook.md", "w") as file_handle:
        file_handle.write(
            """:::ocean_data_parser.read.dfo.odf\n\n## ODF Vocabulary\n"""
        )
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def get_ios_vocab_markdown(version):
    df = pd.read_csv(
        "https://raw.githubusercontent.com/cioos-siooc/cioos-siooc_data_transform/ios-parser-extra-vocabulary/cioos_data_transform/cioos_data_transform/utils/ios_vocabulary.csv"
    )
    for column in [
        "ios_name",
        "accepted_units",
        "accepted_instrument_type",
        "apply_func",
    ]:
        df[column] = quote_column(df[column].astype(str))
    with open(f"{version}/docs/read/dfo/ios-hook.md", "w") as file_handle:
        file_handle.write(
            """:::ocean_data_parser.read.dfo.ios\n\n## IOS Vocabulary\n"""
        )
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def get_amundsen_vocab_markdown(version):
    with open(
        "ocean_data_parser/read/vocabularies/amundsen_variable_attributes.json"
    ) as file_handle:
        vocab = json.load(file_handle)
    df = pd.DataFrame(
        [
            {"Amundsen Name": name, **(attrs if isinstance(attrs, dict) else {})}
            for name, options in vocab.items()
            for attrs in options
            if attrs and name != "VARIABLE_NAME"
        ]
    )
    with open(f"{version}/docs/read/amundsen-hook.md", "w") as file_handle:
        file_handle.write(""":::ocean_data_parser.read.amundsen\n\n## Vocabulary\n""")
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def get_seabird_vocab_markdown(version):
    with open(
        "ocean_data_parser/read/vocabularies/seabird_variable_attributes.json"
    ) as file_handle:
        vocab = json.load(file_handle)
    df = pd.DataFrame(
        [
            {"Seabird Name": name, **(attrs if isinstance(attrs, dict) else {})}
            for name, attrs in vocab.items()
        ]
    )
    with open(f"{version}/docs/read/seabird-hook.md", "w") as file_handle:
        file_handle.write(""":::ocean_data_parser.read.seabird\n\n## Vocabulary\n""")
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def on_pre_build(config, **kwargs) -> None:
    logger.debug("config=%s", config)
    logger.debug("kwargs=%s", kwargs)

    logger.info("Active Branch=%s", get_active_branch_name())
    active_branch = get_active_branch_name()
    logger.info("Active Branch=%s", active_branch)
    version = "main" if active_branch == "main" else "dev"
    get_odf_vocab_markdown(version)
    get_ios_vocab_markdown(version)
    get_amundsen_vocab_markdown(version)
    get_seabird_vocab_markdown(version)
