# Ocean Data Parsers

Each parser can also be imported by itself:

```python
from ocean_data_parser.parsers import dfo.odf

ds = dfo.odf.bio_odf('file_path')
```

## Parsers available

Ocean Data Parser includes the following data format parsers:

[AMUNDSEN](amundsen.md)

- [amundsen.int_format](amundsen.md#ocean_data_parser.parsers.amundsen.int_format)

[DFO.IOS](dfo/ios.md)

- [dfo.ios.shell](dfo/ios.md#ocean_data_parser.parsers.dfo.ios.shell)
- [dfo.ios.shell](dfo/ios.md#ocean_data_parser.parsers.dfo.ios.shell)

[DFO.NAFC](dfo/nafc.md)

- [dfo.nafc.pcnv](dfo/nafc.md#ocean_data_parser.parsers.dfo.nafc.pcnv)
- [dfo.nafc.pfile](dfo/nafc.md#ocean_data_parser.parsers.dfo.nafc.pfile)

[DFO.ODF](dfo/odf.md)

- [dfo.odf.bio_odf](dfo/odf.md#ocean_data_parser.parsers.dfo.odf.bio_odf)
- [dfo.odf.mli_odf](dfo/odf.md#ocean_data_parser.parsers.dfo.odf.mli_odf)
- [dfo.odf.mli_odf](dfo/odf.md#ocean_data_parser.parsers.dfo.odf.mli_odf)

[ELECTRICBLUE](electricblue.md)

- [electricblue.csv](electricblue.md#ocean_data_parser.parsers.electricblue.csv)
- [electricblue.log_csv](electricblue.md#ocean_data_parser.parsers.electricblue.log_csv)

[NETCDF](netcdf.md)

- [netcdf](netcdf.md)

[NMEA](nmea.md)

- [nmea.file](nmea.md#ocean_data_parser.parsers.nmea.file)

[ONSET](onset.md)

- [onset.csv](onset.md#ocean_data_parser.parsers.onset.csv)

[PME](pme.md)

- [pme.minidot_txt](pme.md#ocean_data_parser.parsers.pme.minidot_txt)

[RBR](rbr.md)

- [rbr.rtext](rbr.md#ocean_data_parser.parsers.rbr.rtext)

[SEABIRD](seabird.md)

- [seabird.btl](seabird.md#ocean_data_parser.parsers.seabird.btl)
- [seabird.cnv](seabird.md#ocean_data_parser.parsers.seabird.cnv)

[STAR_ODDI](star-oddi.md)

- [star_oddi.DAT](star-oddi.md#ocean_data_parser.parsers.star_oddi.DAT)

[SUNBURST](sunburst.md)

- [sunburst.superCO2_notes](sunburst.md#ocean_data_parser.parsers.sunburst.superCO2_notes)
- [sunburst.superCO2](sunburst.md#ocean_data_parser.parsers.sunburst.superCO2)

[VAN_ESSEN_INSTRUMENTS](van-essen-instruments.md)

- [van_essen_instruments.mon](van-essen-instruments.md#ocean_data_parser.parsers.van_essen_instruments.mon)

