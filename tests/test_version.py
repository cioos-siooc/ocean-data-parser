from pathlib import Path

import toml

from ocean_data_parser import __version__


def test_pyproject():
    """Test the Python package version"""
    pyproject = toml.loads(Path("pyproject.toml").read_text())
    assert __version__ == pyproject["tool"]["poetry"]["version"]


def test_changelog_version():
    """Test the CHANGELOG version"""
    changelog = Path("CHANGELOG.md").read_text()
    assert f"## `{__version__}`" in changelog
