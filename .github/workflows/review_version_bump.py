import re
import subprocess
import argparse

with open('ocean_data_parser/_version.py') as file:
    version_str = file.read() 

version_regex = r'__version__ = "\d+.\d+.\d+"'

def _get_active_version():
    with open('ocean_data_parser/_version.py') as file:
        return file.read()
    
def _get_active_branch():
    active_branch_output = subprocess.run(["git","rev-parse","--abbrev-ref","HEAD"], capture_output=True)
    if active_branch_output.returncode:
        raise RuntimeError(active_branch_output.stderr.decode())
    return active_branch_output.stdout.decode().strip()

def _compare_versions(from_branch,to_branch):
    version_difference_output = subprocess.run(['git','diff',from_branch,to_branch,'ocean_data_parser/_version.py'], capture_output=True)

    return version_difference_output.stdout.decode().split('\n')[-3:-1]

def review_version(reference_branch= 'main'):
    version_str = _get_active_version()
    assert re.fullmatch(version_regex + r'\n', version_str), f"_version.py has a different format than {version_regex}"

    # Get acgive

    active_branch = _get_active_branch()
    reference_version, active_version = _compare_versions(reference_branch, active_branch)
    
    assert re.search(version_regex,reference_version), f"Failed to detected {reference_version=}"
    assert re.search(version_regex,active_version), f"No version detected {active_version=}"
    assert reference_version[1:] < active_version[1:], f"{reference_version=} is not > {active_version=}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-b",dest="reference_branch", default='main')
    args = parser.parse_args()
    review_version(args.reference_branch)