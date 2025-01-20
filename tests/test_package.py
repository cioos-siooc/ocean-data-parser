from pathlib import Path

import toml

from ocean_data_parser import __version__


def test_pyproject_version():
    pyproject = Path(__file__).parent / ".." / "pyproject.toml"
    project = toml.loads(pyproject.read_text())
    assert project["project"]["version"] == __version__
