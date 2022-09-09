"""This script is used to convert the different INT files 
available within the different datasets maintained by PDC to a NetCDF format.

Those NetCDFs are then served by the PDC Hyrax and CIOOS ERDAP servers
"""

from glob import glob
import logging

from ocean_data_parser.read import amundsen
from ocean_data_parser.metadata import pdc
from tqdm import tqdm

FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(
    filename="pdc-amundsen-conversion.log", level=logging.WARNING, format=FORMAT
)

start_logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(start_logger, {"ccin": None, "path": None})

extra = {"ccin": None, "file": None}
ccinNum = [
    "80",
    "438",
    "449",
    "452",
    "456",
    "468",
    "482",
    "491",
    "496",
    "502",
    "509",
    "510",
    "513",
    "514",
    "515",
    "516",
    "518",
    "519",
    "520",
    "521",
    "522",
    "524",
    "525",
    "526",
    "527",
    "796",
    "797",
    "798",
    "799",
    "800",
    "936",
    "1496",
    "1497",
    "1498",
    "1499",
    "1500",
    "1501",
    "1502",
    "10987",
    "10988",
    "10989",
    "10990",
    "10991",
    "10992",
    "11150",
    "11151",
    "11153",
    "11154",
    "11155",
    "11156",
    "11896",
    "11919",
    "11920",
    "11921",
    "11922",
    "11923",
    "11924",
    "11926",
    "11943",
    "12447",
    "12518",
    "12519",
    "12713",
    "12715",
    "12716",
    "12717",
]

test = ["12713"]


for ccin in test:
    PATH = f"./tests/parsers_test_files/amundsen/{ccin}/**/*[!_info].int"
    fgdc_metadata_url = f"https://www.polardata.ca/pdcsearch/xml/fgdc/{ccin}_fgdc.xml"
    ccin_metadata = pdc.fgdc_to_acdd(url=fgdc_metadata_url)
    # Ignore geospatial and time attributes which are dataset specific
    ccin_metadata = {
        attr: value
        for attr, value in ccin_metadata.items()
        if not attr.startswith(("geospatial_", "time_coverage_"))
    }

    paths = glob(PATH, recursive=True)
    for path in tqdm(paths):
        try:
            ds = amundsen.int_format(path)
            ds.attrs.update(ccin_metadata)
            ds.to_netcdf(f"{path}.nc", format="NETCDF4_CLASSIC")
        except:
            logging.error("Failed to convert ccin %s, path %s", ccin, path)
