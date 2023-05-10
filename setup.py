from setuptools import find_packages, setup

setup(
    name="ocean_data_parser",
    version="0.2.0",
    description="Package used to parse different Ocean Instruments Propriatary format to an xarray dataset.",
    url="https://github.com/HakaiInstitute/ocean-data-parser",
    author="Jessy Barrette",
    author_email="jessy.barrette@hakai.org",
    license="MIT",
    packages=find_packages(),
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
        "lxml",
    ],
    extras_require={
        "odf": ["geographicLib", "shapely"],
        "dev": [
            "geographicLib",
            "shapely",
            "pytest",
            "pylint",
            "flake8",
            "black",
            "pytest-benchmark",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    zip_safe=True,
)
