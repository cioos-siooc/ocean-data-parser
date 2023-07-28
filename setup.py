from setuptools import find_packages, setup

pkg_vars = {}
with open("ocean_data_parser/_version.py") as fp:
    exec(fp.read(), pkg_vars)

setup(
    name="ocean_data_parser",
    version=pkg_vars["__version__"],
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
        "pandas>=1.3, <2.0",
        "xmltodict",
        "tqdm",
        "pytz",
        "NetCDF4",
        "pynmea2",
        "gsw-xarray",
        "tabulate",
        "lxml",
        "pyyaml",
        "click",
        "loguru",
        "python-dotenv",
        "sentry-sdk[loguru]",
        "cioos_data_transform @ git+https://github.com/cioos-siooc/cioos-siooc_data_transform.git@ios-parser-extra-vocabulary#egg=cioos_data_transform&subdirectory=cioos_data_transform",
        "cioos_data_transform_projects @ git+https://github.com/cioos-siooc/cioos-siooc_data_transform.git@ios-parser-extra-vocabulary#egg=cioos_data_transform_projects&subdirectory=projects",
    ],
    extras_require={
        "odf": ["geographicLib", "shapely"],
        "process": ["plotly", "ipywidgets", "IPython", "ipywidgets"],
        "dev": [
            "plotly",
            "ipywidgets",
            "IPython",
            "geographicLib",
            "shapely",
            "pytest",
            "pylint",
            "flake8",
            "black",
            "ioos_qc",
            "pytest-benchmark",
            "mkdocs",
            "mkdocs-material",
            "mkdocstrings[python]",
            "mkdocs-jupyter",
            "mkdocs-gen-files",
            "mkdocs-simple-hooks",
            "mike",
            "tabulate",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    zip_safe=True,
    entry_points={
        "console_scripts": [
            "odpy.convert = ocean_data_parser.batch.convert:cli_files",
            "odpy.compile.nc.variables = ocean_data_parser.compile.netcdf:cli_variables",
        ]
    },
)
