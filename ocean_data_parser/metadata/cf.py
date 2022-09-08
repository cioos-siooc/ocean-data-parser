import requests
import re
import xmltodict


def get_standard_name_latest_table_version():
    response = requests.get("https://cfconventions.org/standard-names.html")
    if response.status_code != 200:
        response.raise_for_status()
    result = re.search(
        r"\(current version, v(?P<version>\d+),\s*(?P<date>\d{1,2} \w+ \d{4})",
        response.text,
    )
    return int(result["version"]), result["date"]


def get_standard_name_table(version="latest"):
    if version == "latest":
        version, date = get_standard_name_latest_table_version()

    url = f"https://cfconventions.org/Data/cf-standard-names/{version}/src/cf-standard-name-table.xml"
    response = requests.get(url)
    if response.status_code != 200:
        response.raise_for_status()
    return xmltodict.parse(response.text)
