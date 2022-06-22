import json
import os

import pandas as pd


def winkler_titration_json(paths, encoding="utf-8"):
    runs = []
    for path in paths:
        with open(path, encoding=encoding) as f:
            df_temp = pd.DataFrame(json.load(f))

            # Use json file name as titration run unique identifier
            df_temp.insert(0, "runID", os.path.basename(path))
            runs += [df_temp]
    df = pd.concat(runs, ignore_index=True)

    # Convert time variables to datetime
    df["init_time"] = pd.to_datetime(df["init_time"], utc=False)
    df["end_time"] = pd.to_datetime(df["end_time"])
    return df


def generate_excel_sheet(data):
    pass
