import logging
import re

import pandas as pd
import requests
import xmltodict

logger = logging.getLogger(__name__)


# def fgdc_to_acdd(xml)
def fgdc_to_acdd(url=None, xml=None):
    """Convert PDC FGDC XML format to an ACDD 1.3 standard dictionary format."""
    # Load from URL if provided
    if url:
        with requests.get(url) as f:
            xml = f.text
    # Convert xml to python dictionary
    info = xmltodict.parse(xml)
    meta_info = info["metadata"]["metainfo"]
    id_info = info["metadata"]["idinfo"]
    dist_info = info["metadata"]["distinfo"]

    ccin = int(dist_info["resdesc"])
    contact_string = meta_info["metc"]["cntinfo"]["cntperp"]["cntper"]

    doi = (
        re.search(r"(http:\/\/doi\.org\/[0-9a-zA-Z\/\.]+)", xml)[1]
        if "http://doi.org" in xml
        else None
    )

    metadata = {
        "title": id_info["citation"]["citeinfo"]["title"],
        "summary": id_info["descript"]["abstract"],
        "comment": "Purpose: {} \n\n Supplementary information: {}".format(
            id_info["descript"]["purpose"],
            id_info["descript"]["supplinf"],
        ),
        "institution": None,
        "date_created": pd.to_datetime(meta_info["metd"]).isoformat(),
        "creator_name": contact_string.split(":")[1]
        if ":" in contact_string
        else contact_string,
        "creator_role": contact_string.split(":")[0] if ":" in contact_string else None,
        "creator_type": "person",
        "creator_email": meta_info["metc"]["cntinfo"]["cntemail"],
        "creator_institution": meta_info["metc"]["cntinfo"]["cntperp"]["cntorg"],
        "contributor_name": "; ".join(list(id_info["citation"]["citeinfo"]["origin"])),
        "contributor_role": "original creators",
        "publisher_name": dist_info["distrib"]["cntinfo"]["cntperp"]["cntper"],
        "publisher_type": "institution",
        "publisher_email": dist_info["distrib"]["cntinfo"]["cntemail"],
        "publisher_institution": dist_info["distrib"]["cntinfo"]["cntperp"]["cntorg"],
        "ccin": ccin,
        "id": ccin,
        "naming_authority": "ca.polardata.ccin",
        "doi": doi,
        "geospatial_lat_min": float(id_info["spdom"]["bounding"]["southbc"]),
        "geospatial_lat_max": float(id_info["spdom"]["bounding"]["northbc"]),
        "geospatial_lat_units": "degree_north",
        "geospatial_lon_min": float(id_info["spdom"]["bounding"]["westbc"]),
        "geospatial_lon_max": float(id_info["spdom"]["bounding"]["eastbc"]),
        "geospatial_lon_units": "degree_east",
        "time_coverage_start": pd.to_datetime(
            id_info["timeperd"]["timeinfo"]["rngdates"]["begdate"]
        ).isoformat(),
        "time_coverage_end": None
        if id_info["timeperd"]["timeinfo"]["rngdates"]["enddate"].startswith("9999")
        else pd.to_datetime(
            id_info["timeperd"]["timeinfo"]["rngdates"]["enddate"]
        ).isoformat(),
        "keywords": "; ".join(
            id_info["keywords"]["theme"]["themekey"]
            + [id_info["keywords"]["place"]["placekt"]]
        ),
        "metadata_link": id_info["citation"]["citeinfo"]["onlink"],
        "infoUrl": id_info["citation"]["citeinfo"]["onlink"],
        "reference": doi or id_info["citation"]["citeinfo"]["onlink"],
    }

    # Review instution
    if re.search("Amundsen|CFL|CASES|ArcticNet", metadata["title"]):
        metadata["institution"] = "Amundsen Sciences"
    else:
        logger.warning("Uknown institution for dataset Title: %s", metadata["title"])

    return {key: value for key, value in metadata.items() if value}
