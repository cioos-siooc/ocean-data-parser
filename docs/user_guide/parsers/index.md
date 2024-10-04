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
- [onset.xlsx](onset.md#ocean_data_parser.parsers.onset.xlsx)

[PME](pme.md)

- [pme.txt](pme.md#ocean_data_parser.parsers.pme.txt)

[RBR](rbr.md)

- [rbr.rtext](rbr.md#ocean_data_parser.parsers.rbr.rtext)

[SEABIRD](seabird.md)

- [seabird.btl](seabird.md#ocean_data_parser.parsers.seabird.btl)
- [seabird.cnv](seabird.md#ocean_data_parser.parsers.seabird.cnv)

[STAR_ODDI](star-oddi.md)

- [star_oddi.dat](star-oddi.md#ocean_data_parser.parsers.star_oddi.dat)

[SUNBURST](sunburst.md)

- [sunburst.super_co2_notes](sunburst.md#ocean_data_parser.parsers.sunburst.super_co2_notes)
- [sunburst.super_co2](sunburst.md#ocean_data_parser.parsers.sunburst.super_co2)

[VAN_ESSEN_INSTRUMENTS](van-essen-instruments.md)

- [van_essen_instruments.mon](van-essen-instruments.md#ocean_data_parser.parsers.van_essen_instruments.mon)

