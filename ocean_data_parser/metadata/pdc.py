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
    ccin = int(info["metadata"]["distinfo"]["resdesc"])
    contact_string = info["metadata"]["metainfo"]["metc"]["cntinfo"]["cntperp"][
        "cntper"
    ]
    doi = (
        re.search(r"(http:\/\/doi\.org\/[0-9a-zA-Z\/\.]+)", xml)[1]
        if "http://doi.org" in xml
        else None
    )

    metadata = {
        "title": info["metadata"]["idinfo"]["citation"]["citeinfo"]["title"],
        "summary": info["metadata"]["idinfo"]["descript"]["abstract"],
        "comment": "Purpose: %s \n\n Supplementary information: %s"
        % (
            info["metadata"]["idinfo"]["descript"]["purpose"],
            info["metadata"]["idinfo"]["descript"]["supplinf"],
        ),
        "institution": None,
        "date_created": pd.to_datetime(
            info["metadata"]["metainfo"]["metd"]
        ).isoformat(),
        "creator_name": contact_string.split(":")[1]
        if ":" in contact_string
        else contact_string,
        "creator_role": contact_string.split(":")[0] if ":" in contact_string else None,
        "creator_type": "person",
        "creator_email": info["metadata"]["metainfo"]["metc"]["cntinfo"]["cntemail"],
        "creator_institution": info["metadata"]["metainfo"]["metc"]["cntinfo"][
            "cntperp"
        ]["cntorg"],
        "contributor_name": "; ".join(
            set(info["metadata"]["idinfo"]["citation"]["citeinfo"]["origin"])
        ),
        "contributor_role": "original creators",
        "publisher_name": info["metadata"]["distinfo"]["distrib"]["cntinfo"]["cntperp"][
            "cntper"
        ],
        "publisher_type": "institution",
        "publisher_email": info["metadata"]["distinfo"]["distrib"]["cntinfo"][
            "cntemail"
        ],
        "publisher_institution": info["metadata"]["distinfo"]["distrib"]["cntinfo"][
            "cntperp"
        ]["cntorg"],
        "ccin": ccin,
        "id": ccin,
        "naming_authority": "ca.polardata.ccin",
        "doi": doi,
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
        "time_coverage_end": None
        if info["metadata"]["idinfo"]["timeperd"]["timeinfo"]["rngdates"][
            "enddate"
        ].startswith("9999")
        else pd.to_datetime(
            info["metadata"]["idinfo"]["timeperd"]["timeinfo"]["rngdates"]["enddate"]
        ).isoformat(),
        "keywords": "; ".join(
            info["metadata"]["idinfo"]["keywords"]["theme"]["themekey"]
            + [info["metadata"]["idinfo"]["keywords"]["place"]["placekt"]]
        ),
        "metadata_link": info["metadata"]["idinfo"]["citation"]["citeinfo"]["onlink"],
        "infoUrl": info["metadata"]["idinfo"]["citation"]["citeinfo"]["onlink"],
        "reference": doi
        or info["metadata"]["idinfo"]["citation"]["citeinfo"]["onlink"],
    }

    # Review instution
    if re.search("Amundsen|CFL|CASES|ArcticNet", metadata["title"]):
        metadata["institution"] = "Amundsen Sciences"
    else:
        logger.warning("Uknown institution for dataset Title: %s", metadata["title"])

    return {key: value for key, value in metadata.items() if value}
