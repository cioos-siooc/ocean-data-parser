import pandas as pd


def get_standard_names(version=81):
    """get_standard_names return a pandas dataframe of the specificied
    version of the cf convention standard_names table.
    Table specific information are made available within the attrs."""

    def _get_value(column):
        return standard_names[column].dropna().unique()[0]

    unique_value_columns = ["version_number", "last_modified", "institution", "contact"]

    standard_names = pd.read_xml(
        f"https://cfconventions.org/Data/cf-standard-names/{version}/src/cf-standard-name-table.xml"
    )
    standard_names.attrs = {item: _get_value(item) for item in unique_value_columns}
    return (
        standard_names.drop(index=[0, 1, 2, 3])
        .drop(columns=unique_value_columns)
        .reset_index(drop=True)
    )
