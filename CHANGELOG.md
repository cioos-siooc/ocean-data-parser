# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## `unreleased`

### Added

- Add odpy convert `input_table` input through config file, which gives the
ability to list multiple file glob expression and associated metadata.
- Add Onset.csv timestamp format: "\d+\/\d+\/\d\d\d\d\s+\d+\:\d+\:\d+" = "%m/%d/%Y %H:%M:%S"
- Rely on ruff for format and linter testing
- Add option to pass a list of input_path paths via the configuration file or a 
os path seperator list via the command line interface or the configuration

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
