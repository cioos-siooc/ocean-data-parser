import pandas as pd
import numpy as np
import json


def quote_column(col):
    return "`" + col + "`"


def get_odf_vocab_markdown():
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
    with open("docs/read/dfo/odf-hook.md", "w") as file_handle:
        file_handle.write(
            """:::ocean_data_parser.read.dfo.odf\n\n## ODF Vocabulary\n"""
        )
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
    with open("docs/read/dfo/ios-hook.md", "w") as file_handle:
        file_handle.write(
            """:::ocean_data_parser.read.dfo.ios\n\n## IOS Vocabulary\n"""
        )
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def get_amundsen_vocab_markdown():
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
    with open("docs/read/amundsen-hook.md", "w") as file_handle:
        file_handle.write(""":::ocean_data_parser.read.amundsen\n\n## Vocabulary\n""")
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def get_seabird_vocab_markdown():
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
    with open("docs/read/seabird-hook.md", "w") as file_handle:
        file_handle.write(""":::ocean_data_parser.read.seabird\n\n## Vocabulary\n""")
        df.replace({np.nan: ""}).to_markdown(file_handle, index=False, tablefmt="pipe")


def on_pre_build(config, **kwargs) -> None:
    get_odf_vocab_markdown()
    get_ios_vocab_markdown()
    get_amundsen_vocab_markdown()
    get_seabird_vocab_markdown()
