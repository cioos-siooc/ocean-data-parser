from setuptools import setup, find_packages

setup(
    name="ocean_data_parser",
    version="0.1.0",
    description="Package used to parse different Ocean Instruments Propriatary format to an xarray dataset.",
    url="https://github.com/HakaiInstitute/ocean-data-parser",
    author="Jessy Barrette",
    author_email="jessy.barrette@hakai.org",
    license="MIT",
    packages=["ocean_data_parser"],
    include_package_data=True,
    install_requires=[
        "numpy",
        "xarray",
        "requests",
        "pandas",
        "xmltodict",
        "tqdm",
        "pytz",
        "NetCDF4",
        "pynmea2",
        "gsw",
    ],
    extras={"odf": ["geographicLib", "shapely"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    zip_safe=True,
)
