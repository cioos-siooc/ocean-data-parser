import pytest

from ocean_data_parser.metadata.cf import warn_if_outdated


def pytest_addoption(parser):
    parser.addoption(
        "--nerc-vocab",
        action="store_true",
        dest="nerc_vocab",
        default=False,
        help="enable nerc vocabulary tests",
    )
    parser.addoption(
        "--cf-version-check",
        action="store_true",
        default=False,
        help="warn if a newer CF Standard Name Table is published on cfconventions.org",
    )


@pytest.fixture(scope="session", autouse=True)
def _check_cf_standard_names_version(request):
    """Warn once per test session if a newer CF Standard Name Table is available."""
    if request.config.getoption("cf_version_check"):
        warn_if_outdated()
