
from pathlib import Path
from ocean_data_parser import __version__
import toml

def test_pyproject_version():
    pyproject = Path(__file__).parent / ".." / "pyproject.toml"
    project = toml.loads(pyproject.read_text())
    assert project["tool"]["poetry"]["version"] == __version__
