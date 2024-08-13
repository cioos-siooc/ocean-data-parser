import pandas as pd

def xlsx(file:str, global_attributes: dict = None, sheet_name: str = "Sheet1", header=2, index_col=False):
    """Parse HACH TR9100 Portable Spectrophotometer xlsx file format.
    Args:
        file (str): Path to file
        global_attributes (dict): Global attributes to add to the dataset
    Returns:
        xarray.Dataset: Dataset with the parsed data
    """
    df = pd.read_excel(file, sheet_name=sheet_name, header=header, index_col=index_col)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace('#', 'number')
    ds = df.to_xarray()
    ds.attrs = {
        **(global_attributes or {}),
        "source_file": file,
        "source_file_sheet_name": sheet_name,
        "parser": "turner.xlsx",
    }
    return ds