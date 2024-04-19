# Ocean Data Parsers 

Each parser can also be imported by itself:

```python
from ocean_data_parser.parsers import dfo.odf

ds = dfo.odf.bio_odf('file_path')
```

## Parsers available

Ocean Data Parser includes the following data format parsers:

[AMUNDSEN](amundsen)

- [amundsen.int_format](amundsen/#ocean_data_parser.parsers.amundsen.int_format)

[DFO.IOS](dfo/ios)

- [dfo.ios.shell](dfo/ios/#ocean_data_parser.parsers.dfo.ios.shell)
- [dfo.ios.shell](dfo/ios/#ocean_data_parser.parsers.dfo.ios.shell)

[DFO.NAFC](dfo/nafc)

- [dfo.nafc.pcnv](dfo/nafc/#ocean_data_parser.parsers.dfo.nafc.pcnv)
- [dfo.nafc.pfile](dfo/nafc/#ocean_data_parser.parsers.dfo.nafc.pfile)

[DFO.ODF](dfo/odf)

- [dfo.odf.bio_odf](dfo/odf/#ocean_data_parser.parsers.dfo.odf.bio_odf)
- [dfo.odf.mli_odf](dfo/odf/#ocean_data_parser.parsers.dfo.odf.mli_odf)
- [dfo.odf.mli_odf](dfo/odf/#ocean_data_parser.parsers.dfo.odf.mli_odf)

[ELECTRICBLUE](electricblue)

- [electricblue.csv](electricblue/#ocean_data_parser.parsers.electricblue.csv)
- [electricblue.log_csv](electricblue/#ocean_data_parser.parsers.electricblue.log_csv)

[NETCDF](netcdf)

- netcdf

[NMEA](nmea)

- [nmea.file](nmea/#ocean_data_parser.parsers.nmea.file)

[ONSET](onset)

- [onset.csv](onset/#ocean_data_parser.parsers.onset.csv)

[PME](pme)

- [pme.minidot_txt](pme/#ocean_data_parser.parsers.pme.minidot_txt)

[RBR](rbr)

- [rbr.rtext](rbr/#ocean_data_parser.parsers.rbr.rtext)

[SEABIRD](seabird)

- [seabird.btl](seabird/#ocean_data_parser.parsers.seabird.btl)
- [seabird.cnv](seabird/#ocean_data_parser.parsers.seabird.cnv)

[STAR_ODDI](star_oddi)

- [star_oddi.DAT](star_oddi/#ocean_data_parser.parsers.star_oddi.DAT)

[SUNBURST](sunburst)

- [sunburst.superCO2_notes](sunburst/#ocean_data_parser.parsers.sunburst.superCO2_notes)
- [sunburst.superCO2](sunburst/#ocean_data_parser.parsers.sunburst.superCO2)

[VAN_ESSEN_INSTRUMENTS](van_essen_instruments)

- [van_essen_instruments.mon](van_essen_instruments/#ocean_data_parser.parsers.van_essen_instruments.mon)


