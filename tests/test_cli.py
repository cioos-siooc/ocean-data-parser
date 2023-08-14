import pytest
import os
from click.testing import CliRunner
from ocean_data_parser import cli, __version__
from ocean_data_parser.batch import convert
from ocean_data_parser.compile.netcdf import compile


def run_command(func, args, env=None, isolated_directory=None):
    """Run Click cli code"""
    runner = CliRunner()
    with runner.isolated_filesystem(isolated_directory or os.getcwd()):
        return runner.invoke(func, args, env=env)


@pytest.mark.parametrize(
    "args,expected_output",
    (
        ("--version", "version"),
        ("--help", "Usage:"),
        ("--show-arguments", "odpy parameters inputs:"),
        (["--show-arguments", "--log-level=WARNING"], "log_level=WARNING"),
        (["--show-arguments", "--log-level", "WARNING"], "log_level=WARNING"),
    ),
)
def test_odpy_main_arguments(args, expected_output):
    results = run_command(cli.main, args)
    assert expected_output in results.output


@pytest.mark.parametrize(
    "env,expected_output",
    (
        ({"ODPY_LOG_LEVEL": "WARNING"}, "log_level=WARNING"),
        ({"ODPY_LOG_FILE": "test.log"}, "log_file=test.log"),
    ),
)
def test_odpy_main_args_from_env_variables(env, expected_output):
    results = run_command(cli.main, "--show-arguments", env)
    assert expected_output in results.output


@pytest.mark.parametrize(
    "args,expected_output",
    (
        ("--version", "version"),
        ("--help", "Usage:"),
        ("--show-arguments=stop", "odpy convert parameter inputs:"),
        (["--show-arguments=stop", "--input-path=test.csv"], "input_path=test.csv"),
        (["--show-arguments=stop", "--input-path", "test.csv"], "input_path=test.csv"),
    ),
)
def test_odpy_convert_arguments(args, expected_output):
    results = run_command(convert.convert, args)
    assert expected_output in results.output


@pytest.mark.parametrize(
    "env,expected_output",
    (
        ({"ODPY_CONVERT_INPUT_PATH": "test.csv"}, "input_path=test.csv"),
        ({"ODPY_CONVERT_PARSER": "onset.csv"}, "parser=onset.csv"),
        ({"ODPY_CONVERT_OVERWRITE": "true"}, "overwrite=True"),
        ({"ODPY_CONVERT_MULTIPROCESSING": "3"}, "multiprocessing=3"),
        ({"ODPY_CONVERT_ERRORS": "raise"}, "errors=raise"),
    ),
)
def test_odpy_convert_args_from_env_variables(env, expected_output):
    results = run_command(convert.convert, "--show-arguments=stop", env)
    assert expected_output in results.output


@pytest.mark.parametrize(
    "args,env,expected_output",
    (
        (["--show-arguments=stop",'-i','good.csv'],{"ODPY_CONVERT_INPUT_PATH": "notgood.csv"}, "input_path=good.csv"),
        (["--show-arguments=stop",'--input-path','good.csv'],{"ODPY_CONVERT_INPUT_PATH": "notgood.csv"}, "input_path=good.csv"),
        (["--show-arguments=stop",'--input-path=good.csv'],{"ODPY_CONVERT_INPUT_PATH": "notgood.csv"}, "input_path=good.csv"),
    ),
)
def test_odpy_convert_args_and_env_variables(args,env, expected_output):
    results = run_command(convert.convert, args, env)
    assert expected_output in results.output

@pytest.mark.parametrize(
    "args,expected_output",
    (
        ("--version", "version"),
        ("--help", "Usage:"),
        ("--show-arguments=stop", "odpy convert parameter inputs:"),
        (["--show-arguments=stop", "--input=test.nc"], "input=test.nc"),
    ),
)
def test_odpy_convert_arguments(args, expected_output):
    results = run_command(compile, args)
    assert expected_output in results.output