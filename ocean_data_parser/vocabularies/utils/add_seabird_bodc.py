import pandas as pd
import json
from loguru import logger

columns = {'BODC preferred label':"sdn_parameter_name", 'BODC Identifier':"sdn_parameter_urn", 'BODC Alternative Name':"bodc_alternative_label"}

def update_seabird_vocabulary():
    with open("ocean_data_parser/vocabularies/seabird_vocabulary.json") as file_handle:
        vocab = json.load(file_handle)
    
    # Load BODC vocabulart
    bodc = pd.read_excel("ocean_data_parser\\vocabularies\\BODC_vocabularies_dictionnary.xlsx").set_index("Variable_BTL")
    bodc = bodc.rename(columns=columns)
    
    for variable, attrs in bodc.iterrows():
        if pd.isna(variable):
            continue
        if variable not in vocab:
            variable= variable[0].lower() + variable[1:]

        if variable not in vocab:
            logger.warning(f"Variable {variable} not found in Seabird vocabulary")
            continue
        vocab[variable].update(attrs[columns.values()].dropna().apply(lambda x: x.strip()).to_dict())

    with open("ocean_data_parser/vocabularies/seabird_vocabulary.json", "w") as file_handle:
        json.dump(vocab, file_handle, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    update_seabird_vocabulary()
    