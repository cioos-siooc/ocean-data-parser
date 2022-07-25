import pandas as pd
import xmltodict
import requests
import json


# def fgdc_to_acdd(xml)
def fgdc_to_acdd(url=None, xml=None):
    """Convert PDC FGDC XML format to an ACDD 1.3 standard dictionary format."""
    # Load from URL if provided
    if url:
        with requests.get(url) as f:
            xml = f.text
    # Convert xml to python dictionary
    info = xmltodict.parse(xml)
    return {
        "title": info["metadata"]["idinfo"]["citation"]["citeinfo"]["title"],
        "summary": info["metadata"]["idinfo"]["descript"]["abstract"],
        "comment": info["metadata"]["idinfo"]["descript"]["supplinf"],
        "institution": "",
        "creator_name": ", ".join(
            set(info["metadata"]["idinfo"]["citation"]["citeinfo"]["origin"])
        ),
        "creator_type": "group",
        "creator_email": "email",
        "publisher_name": info["metadata"]["distinfo"]["distrib"]["cntinfo"]["cntperp"][
            "cntper"
        ],
        "publisher_type": "institution",
        "publisher_email": info["metadata"]["distinfo"]["distrib"]["cntinfo"][
            "cntemail"
        ],
        "geospatial_lat_min": float(
            info["metadata"]["idinfo"]["spdom"]["bounding"]["southbc"]
        ),
        "geospatial_lat_max": float(
            info["metadata"]["idinfo"]["spdom"]["bounding"]["northbc"]
        ),
        "geospatial_lat_units": "degree_north",
        "geospatial_lon_min": float(
            info["metadata"]["idinfo"]["spdom"]["bounding"]["westbc"]
        ),
        "geospatial_lon_max": float(
            info["metadata"]["idinfo"]["spdom"]["bounding"]["eastbc"]
        ),
        "geospatial_lon_units": "degree_east",
        "time_coverage_start": pd.to_datetime(
            info["metadata"]["idinfo"]["timeperd"]["timeinfo"]["rngdates"]["begdate"]
        ).isoformat(),
        "time_coverage_end": pd.to_datetime(
            info["metadata"]["idinfo"]["timeperd"]["timeinfo"]["rngdates"]["enddate"]
        ).isoformat(),
        "keywords": ", ".join(
            info["metadata"]["idinfo"]["keywords"]["theme"]["themekey"]
        ),
        "metadata_link": info["metadata"]["idinfo"]["citation"]["citeinfo"]["onlink"],
        "reference": info["metadata"]["idinfo"]["citation"]["citeinfo"]["onlink"],
    }
