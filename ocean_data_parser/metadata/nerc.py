import json
import requests


def get_variable(vocabulary, variable, version="current"):
    jsonld_url = f"http://vocab.nerc.ac.uk/collection/{vocabulary}/{version}/{variable}/?_profile=nvs&_mediatype=application/ld+json"
    response = requests.get(jsonld_url)
    return (
        json.loads(response.text)
        if response.status_code == 200
        else response.raise_for_status()
    )


def generate_cf_attributes(attributes):
    """Generate the CF and SeaDataNet recommanded attributes from the NERC vocabulary."""
    cf = {
        "sdn_parameter_urn": attributes.get("dc:identifier")
        or attributes.get("dce:identifier"),
        "sdn_parameter_name": attributes["prefLabel"]["@value"],
        "bodc_alternative_label": attributes["altLabel"],
        "definition": attributes["definition"]["@value"],
    }
    return {key: value for key, value in cf if value not in ("Unavailable")}
