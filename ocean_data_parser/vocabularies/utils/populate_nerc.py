import json
from pathlib import Path

from ocean_data_parser.metadata.nerc import Nerc

nerc = Nerc()


p01_vocabulary = nerc.get_p01_vocabulary().set_index("sdn_parameter_urn")
p06_vocabulary = nerc.get_p06_vocabulary().set_index("sdn_uom_urn")

vocabulary_path = Path("ocean_data_parser/vocabularies/amundsen_vocabulary.json")


def populate_amundsen_vocab():
    """Load the Amundsen vocabulary and populate it with the P01 and P06 vocabularies names."""
    def _set_names(attrs):
        if "sdn_parameter_urn" in attrs:
            attrs["sdn_parameter_name"] = p01_vocabulary.at[attrs["sdn_parameter_urn"], "sdn_parameter_name"]
        if "sdn_uom_urn" in attrs:
            attrs["sdn_uom_name"] = p06_vocabulary.at[attrs["sdn_uom_urn"], "sdn_uom_name"]
        return attrs
    
    with open(vocabulary_path) as f:
        amundsen = json.load(f)

    # populate amundsen json
    for variable, attrs in amundsen.items():
        if variable == "VARIABLE_NAME":
            continue
        if isinstance(attrs, dict):
            attrs = _set_names(attrs)
            continue
            
        for attr in attrs:
            attr = _set_names(attr)

    # save amundsen json
    with open(vocabulary_path, "w") as f:
        json.dump(amundsen, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    populate_amundsen_vocab()
