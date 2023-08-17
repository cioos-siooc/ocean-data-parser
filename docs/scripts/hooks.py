import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from ocean_data_parser import PARSERS


def quote_column(col):
    return "`" + col + "`"

def add_vocabularies_dir():
    vocab_dir = Path('docs/user_guide/vocabularies')
    if not vocab_dir.exists():
        vocab_dir.mkdir()

def get_dfo_pfile_vocab_markdown(output="docs/user_guide/vocabularies/dfo-nafc-p-files.md"):
    """Convert P file vocabulary to markdown table"""
    df = pd.read_csv("ocean_data_parser/vocabularies/dfo_nafc_p_files_vocabulary.csv")
    for column in ["accepted_instruments"]:
        df[column] = quote_column(df[column])
    df.replace({np.nan: ""}).to_markdown(output, index=False, tablefmt="pipe")


def get_odf_vocab_markdown(output="docs/user_guide/vocabularies/dfo-odf.md"):
    df = pd.read_csv("ocean_data_parser/vocabularies/dfo_odf_vocabulary.csv")
    for column in [
        "accepted_units",
        "accepted_scale",
        "accepted_instruments",
        "apply_function",
    ]:
        df[column] = quote_column(df[column])

    df.replace({np.nan: ""}).to_markdown(output, index=False, tablefmt="pipe")


def get_ios_vocab_markdown(
    output="docs/user_guide/vocabularies/dfo-ios-shell.md",
):
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
    df.replace({np.nan: ""}).to_markdown(output, index=False, tablefmt="pipe")


def get_amundsen_vocab_markdown(output="docs/user_guide/vocabularies/amundsen-int.md"):
    with open("ocean_data_parser/vocabularies/amundsen_vocabulary.json") as file_handle:
        vocab = json.load(file_handle)
    df = pd.DataFrame(
        [
            {"Amundsen Name": name, **(attrs if isinstance(attrs, dict) else {})}
            for name, options in vocab.items()
            for attrs in options
            if attrs and name != "VARIABLE_NAME"
        ]
    )
    df.replace({np.nan: ""}).to_markdown(output, index=False, tablefmt="pipe")


def get_seabird_vocab_markdown(output="docs/user_guide/vocabularies/seabird.md"):
    with open("ocean_data_parser/vocabularies/seabird_vocabulary.json") as file_handle:
        vocab = json.load(file_handle)
    df = pd.DataFrame(
        [
            {"Seabird Name": name, **(attrs if isinstance(attrs, dict) else {})}
            for name, attrs in vocab.items()
        ]
    )
    df.replace({np.nan: ""}).to_markdown(output, index=False, tablefmt="pipe")


def copy_notebooks(output="docs/notebooks"):
    """Copy notebooks to docs"""
    notebooks = Path("notebooks").glob("*.ipynb")
    docs_notebooks = Path(output)
    docs_notebooks.mkdir(parents=True, exist_ok=True)
    for notebook in notebooks:
        shutil.copy(notebook, docs_notebooks / notebook.name)


def get_parser_list(output="docs/user_guide/parsers/index.md"):
    with open(output, "w") as file:
        file.write("# Parsers available\n")
        file.write("\n".join([f"- {parser}" for parser in PARSERS]))


def on_pre_build(config, **kwargs) -> None:
    add_vocabularies_dir()
    get_odf_vocab_markdown()
    get_ios_vocab_markdown()
    get_amundsen_vocab_markdown()
    get_seabird_vocab_markdown()
    get_dfo_pfile_vocab_markdown()
    copy_notebooks()
    get_parser_list()
