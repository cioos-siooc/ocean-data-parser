# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## `0.7.0`

### Added

- Add compatibility with PME wipers txt format.
- Use uv as package manager
- Add vocabularies tests

### Fixed

- Fixed warning regarding star_oddi dayfirst=True missing input
- Rename pme parsers by removing `minidot_`. New functions are called `pme.txt`,
  `pme.txts`, `pme.cat`. Maintain still a placeholder for those functions.
- Major refactor the whole code base to handle a number of issues raised by Ruff.
- Make tests compatible with windows environment.
- Fixed typo in NAFC pcnv parser, new sample files added to test files

## `0.6.1` - 2024-08-30

### Added

- Add `onset.xlsx` parser.
- Make `onset.xlsx` and `onset.csv` raise a `pytz.exception.AmbiguousTimeError`
  when jumps associated with daylight saving time changes are detected.
- Add `star_oddi.DAT` ctd test file and fix timestamp format handling.

## `0.6.0` - 2024-08-20

### Added

- Add odpy convert `input_table` input through config file, which gives the
  ability to list multiple file glob expression and associated metadata.
- Add Onset.csv timestamp format: "\d+\/\d+\/\d\d\d\d\s+\d+\:\d+\:\d+" = "%m/%d/%Y %H:%M:%S"
- Rely on ruff for format and linter testing
- Add option to pass a list of input_path paths via the configuration file or a
  os path seperator list via the command line interface or the configuration
- Add test to test version within package `__init__.py`, `CHANGELOG.md`, and `pyproject.tom`

### Fixed

- seabird parsers module import sorting
- nafc ruff check issues

## `0.5.2` - 2024-06-22

### Fixed

- Docs build

## `0.5.1` - 2024-06-20

### Fixed

- Remove default sentry dsn from convert default configuration file.
- Fix deprecation issue in nafc.metqa loader

## `0.5.0` - 2024-06-20

### Added

- Test get_path_generation_input on all test files and parsers
- Run all tests in parallel with pytest-xdist
- Add --parser-kwargs option to odpy convert command line interface to pass
  inputs to parser

### Fixed

- Fix star_oddi parser time variable to output a datetime dataarray.
- Fix onset parser date time handling.
- Avoid reimporting parser if already imported in read.file with parser
  defined via string expression.
- Fix Amundsen int timestamp format
- Drop trip_tag attribute from dfo.nafc.pcnv parser
- Make dfo.nafc.pcnv parser attempt to retrieve metqa file info by default.

## `v0.4.0` - 2024-05-04

### Added

- Test platforms vocabulary items and match them to NERC C17 vocabulary.

### Changed

- Refactor Seabird Parser
  - Handle better processing related attributes
    - On-platform processing
    - SBE Data Processing modules
    - Post processing (NAFC pcnv Format `* QA Applied:`)
  - Retrieve more of the information available stored within Seabird header as attributes
  - Structure history
- Upgrade odf.bio reference NetCDF to reflect the changes.
- Upgrate dfo platforms for the odf and nafc parsers.

### Removed

- Historical ODF platform vocabulary which wasn't used anywhere.
