# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## `development` - ...

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