import hooks


def test_get_dfo_pfile_vocab_markdown(tmp_path):
    file = tmp_path / "index.md"
    hooks.get_dfo_pfile_vocab_markdown(file)
    assert file.exists()


def test_get_odf_vocab_markdown(tmp_path):
    file = tmp_path / "index.md"
    hooks.get_odf_vocab_markdown(file)
    assert file.exists()


def test_get_ios_vocab_markdown(tmp_path):
    file = tmp_path / "index.md"
    hooks.get_ios_vocab_markdown(file)
    assert file.exists()


def test_get_amundsen_vocab_markdown(tmp_path):
    file = tmp_path / "index.md"
    hooks.get_amundsen_vocab_markdown(file)
    assert file.exists()


def test_get_seabird_vocab_markdown(tmp_path):
    file = tmp_path / "index.md"
    hooks.get_seabird_vocab_markdown(file)
    assert file.exists()


def test_get_seabird_vcopy_notebooksocab_markdown(tmp_path):
    hooks.copy_notebooks(tmp_path)
    assert tmp_path.glob("*.ipynb")


def test_parser_list(tmp_path):
    file = tmp_path / "index.md"
    hooks.get_parser_list(file)
    assert file.exists()
