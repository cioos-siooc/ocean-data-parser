This repo is meant to allow conversion of standard ocean instruments or institutions file formats to a NetCDF CF 1.6, ACDD 1.3 compliant format. 

## Requirements

- All parsers should have tests files.
- For each parsers generate a odpy.read entry.
- Attempt to add for each variables attributes:
  - standard_name
  - units
  - sdn_parameter_urn (dfo, amundsen only)
  - sdn_parameter_name (dfo, amundsen only)
  - sdn_uom_urn (dfo, amundsen only)
  - sdn_uom_name (dfo, amundsen only)
- time variables should be translated to UTC