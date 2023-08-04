import argparse
from pathlib import Path
import re
import subprocess

version_str = Path("ocean_data_parser/_version.py").read_text()
version_regex = r'__version__ = "\d+.\d+.\d+"'


def _get_active_branch():
    active_branch_output = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True
    )
    if active_branch_output.returncode:
        raise RuntimeError(active_branch_output.stderr.decode())
    return active_branch_output.stdout.decode().strip()


def _compare_versions(from_branch, to_branch):
    version_difference_output = subprocess.run(
        ["git", "diff", f"origin/{from_branch}", f"origin/{to_branch}", "ocean_data_parser/_version.py"],
        capture_output=True,
    )
    assert version_difference_output.stdout.decode()
    return version_difference_output.stdout.decode().split("\n")[-3:-1]


def main(reference_branch="main"):
    assert re.fullmatch(
        version_regex + r"\n", version_str
    ), f"_version.py has a different format than {version_regex}"

    # Get acgive

    active_branch = _get_active_branch()
    reference_version, active_version = _compare_versions(
        reference_branch, active_branch
    )

    assert re.search(
        version_regex, reference_version
    ), f"Failed to detected {reference_version=}"
    assert re.search(
        version_regex, active_version
    ), f"No version detected {active_version=}"
    assert (
        reference_version[1:] < active_version[1:]
    ), f"{reference_version=} is not > {active_version=}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", dest="reference_branch", default="main")
    args = parser.parse_args()
    main(args.reference_branch)
