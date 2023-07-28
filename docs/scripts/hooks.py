import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd


def quote_column(col):
    return "`" + col + "`"


def get_dfo_pfile_vocab_markdown():
    """Convert P file vocabulary to markdown table"""
    df = pd.read_csv(
        "ocean_data_parser/parsers/vocabularies/dfo_p_files_vocabulary.csv"
    )
    for column in ["accepted_instruments"]:
        df[column] = quote_column(df[column])
    with open("docs/parsers/dfo/p-header.md") as file_handle:
        header = file_handle.read()
    with open("docs/parsers/dfo/p-hook.md", "w") as file_handle:
        file_handle.write(header)
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def get_odf_vocab_markdown():
    df = pd.read_csv(
        "ocean_data_parser/parsers/dfo/odf_source/references/reference_vocabulary.csv"
    )
    for column in [
        "accepted_units",
        "accepted_scale",
        "accepted_instruments",
        "apply_function",
    ]:
        df[column] = quote_column(df[column])
    with open("docs/parsers/dfo/odf-header.md") as file_handle:
        header = file_handle.read()
    with open("docs/parsers/dfo/odf-hook.md", "w") as file_handle:
        file_handle.write(header)
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def get_ios_vocab_markdown():
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
    with open("docs/parsers/dfo/ios-header.md") as file_handle:
        header = file_handle.read()
    with open("docs/parsers/dfo/ios-hook.md", "w") as file_handle:
        file_handle.write(header)
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def get_amundsen_vocab_markdown():
    with open(
        "ocean_data_parser/parsers/vocabularies/amundsen_variable_attributes.json"
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
    with open("docs/parsers/amundsen-hook.md", "w") as file_handle:
        file_handle.write(
            """:::ocean_data_parser.parsers.amundsen\n\n## Vocabulary\n"""
        )
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def get_seabird_vocab_markdown():
    with open(
        "ocean_data_parser/parsers/vocabularies/seabird_variable_attributes.json"
    ) as file_handle:
        vocab = json.load(file_handle)
    df = pd.DataFrame(
        [
            {"Seabird Name": name, **(attrs if isinstance(attrs, dict) else {})}
            for name, attrs in vocab.items()
        ]
    )
    with open("docs/parsers/seabird-hook.md", "w") as file_handle:
        file_handle.write(""":::ocean_data_parser.parsers.seabird\n\n## Vocabulary\n""")
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def copy_notebooks():
    """Copy notebooks to docs"""
    notebooks = Path("notebooks").glob("*.ipynb")
    docs_notebooks = Path("docs/notebooks")
    docs_notebooks.mkdir(parents=True, exist_ok=True)
    for notebook in notebooks:
        shutil.copy(notebook, docs_notebooks / notebook.name)


def on_pre_build(config, **kwargs) -> None:
    get_odf_vocab_markdown()
    get_ios_vocab_markdown()
    get_amundsen_vocab_markdown()
    get_seabird_vocab_markdown()
    get_dfo_pfile_vocab_markdown()
    copy_notebooks()
