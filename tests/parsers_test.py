from ocean_data_parser import read
import unittest
from glob import glob

class PMEParserTests(unittest.TestCase):
    def test_txt_parser(self):
        paths = glob("tests/parsers_test_files/pme")
        read.pme.minidot_txts(paths)

class SeabirdParserTests(unittest.TestCase):
    def test_btl_parser(self):
        paths = glob('tests/parsers_test_files/seabird/*.btl')
        for path in paths:
            read.seabird.btl(path)

    def test_cnv_parser(self):
        paths = glob('tests/parsers_test_files/seabird/*.cnv')
        for path in paths:
            read.seabird.cnv(path)

class VanEssenParserTests(unittest.TestCase):
    def test_mon_parser(self):
        paths = glob('tests/parsers_test_files/van_essen_instruments/ctd_divers/*.MON')
        for path in paths:
            read.van_essen_instruments.MON(path)

class OnsetParserTests(unittest.TestCase):
    def test_csv_parser(self):
        paths = glob('tests/parsers_test_files/onset/**/*.csv')
        for path in paths:
            read.onset.csv(path)

class RBRParserTests(unittest.TestCase):
    def test_reng_parser(self):
        paths = glob('tests/parsers_test_files/rbr/*.txt')
        for path in paths:
            read.rbr.rtext(path)
