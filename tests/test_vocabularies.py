import pandas as pd
from ocean_data_parser.vocabularies import load

def test_amundsen_vocabulary_load():
    vocab = load.amundsen_vocabulary()
    assert vocab
    assert isinstance(vocab,dict)

def test_seabird_vocabulary_load():
    vocab = load.seabird_vocabulary()
    assert vocab
    assert isinstance(vocab,dict)

def test_dfo_platform_default_load():
    vocab = load.dfo_platforms()
    assert isinstance(vocab, pd.DataFrame)
    assert not vocab.empty
    assert vocab.index.name == 'platform_name'

def test_dfo_platform_load_nafc():
    vocab = load.dfo_platforms("dfo_newfoundland_ship_code")
    assert isinstance(vocab, pd.DataFrame)
    assert not vocab.empty
    assert vocab.index.name == 'dfo_newfoundland_ship_code'

def test_dfo_odf_vocabulary_load():
    vocab = load.dfo_odf_vocabulary()
    assert isinstance(vocab, pd.DataFrame)
    assert not vocab.empty

def test_dfo_nafc_pfile_vocabulary_load():
    vocab = load.dfo_nafc_p_file_vocabulary()
    assert isinstance(vocab, pd.DataFrame)
    assert not vocab.empty