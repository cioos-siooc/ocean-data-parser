"""This is the ODPY.cli main docstring."""

import logging
import os
import sys

import click
from loguru import logger

from ocean_data_parser import __version__
from ocean_data_parser.batch import convert
from ocean_data_parser.inspect import inspect_variables as inspect_variables

LOG_LEVELS = ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR"]
VERBOSE_LOG_FORMAT = (
    '<level>{level.icon}</level> <blue>"{file.path}"</blue>: '
    '<yellow>line {line}</yellow> | <cyan>"{extra[source_file]}"</cyan> - '
    "<level>{message}</level>"
)
LOG_FORMAT = (
    '<level>{level.icon}</level> <cyan>"{extra[source_file]}"</cyan> - '
    "<level>{message}</level>"
)
logger.remove()
logger.configure(extra={"source_file": ""})


# Redirect logging loggers to loguru
class InterceptHandler(logging.Handler):
    """Redirect logging loggers to loguru.

    # https://stackoverflow.com/questions/66769431/how-to-use-loguru-with-standard-loggers.
    """

    @logger.catch(default=True, onerror=lambda _: sys.exit(1))
    def emit(self, record):
        # Get corresponding Loguru level if it exists.
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


logging.basicConfig(
    handlers=[InterceptHandler()], level=os.getenv("LOGURU_LEVEL", "INFO")
)
classic_logger = logging.getLogger()


@click.group(
    name="odpy",
    invoke_without_command=True,
    context_settings={"auto_envvar_prefix": "ODPY"},
)
@click.version_option(__version__, package_name="ocean-data-parser")
@click.option(
    "--verbose",
    is_flag=True,
    help="Logger format present more information",
    default=False,
)
@click.option(
    "--log-level",
    type=click.Choice(LOG_LEVELS),
    help="Logger level used",
    default="INFO",
    envvar="ODPY_LOG_LEVEL",
    show_default=True,
)
@click.option(
    "--log-file", type=click.Path(), help="Log to a file.", envvar="ODPY_LOG_FILE"
)
@click.option(
    "--log-file-level",
    type=click.Choice(LOG_LEVELS),
    help="Log file level used",
    default="INFO",
    envvar="ODPY_LOG_FILE_LEVEL",
    show_default=True,
)
@click.option(
    "--log-file-rotation",
    type=str,
    help="Rotate log file at a given interval. Given value must be compatible with pandas.TimeDelta",
    default=None,
)
@click.option(
    "--log-file-retention",
    type=str,
    help=(
        "Delete log file after a given time period. "
        "Given value must be compatible with pandas.TimeDelta (e.g. '1D', '1W'). "
        "If None, file will be kept indefinitely."
    ),
    default=None,
)
@click.option(
    "--diagnose/--no-diagnose",
    is_flag=True,
    default=True,
    help="Run loguru diagnose on errors, to see all variable inputs and stacktrace",
    show_default=True,
)
@click.option(
    "--backtrace/--no-backtrace",
    is_flag=True,
    default=True,
    help="Show stacktrace on error",
    show_default=True,
)
@click.option(
    "--backtrace-limit",
    type=int,
    default=5,
    help="Limit stacktrace to N lines",
    show_default=True,
)
@click.option(
    "--show-arguments",
    is_flag=True,
    default=False,
    help="Print present argument values",
    hidden=True,
)
def main(
    verbose,
    log_level,
    log_file,
    log_file_level,
    log_file_rotation,
    log_file_retention,
    backtrace,
    backtrace_limit,
    diagnose,
    show_arguments,
):
    """Ocean Data Parser command line main interface."""
    log_format = VERBOSE_LOG_FORMAT if verbose else LOG_FORMAT
    logger.add(
        sys.stderr,
        level=log_level,
        backtrace=backtrace,
        diagnose=diagnose,
        format=VERBOSE_LOG_FORMAT if verbose else LOG_FORMAT,
    )
    sys.backtrace_limit = backtrace_limit
    if log_file:
        logger.add(
            log_file,
            level=log_file_level,
            format=log_format,
            rotation=log_file_rotation,
            retention=log_file_retention,
            backtrace=backtrace,
            diagnose=diagnose,
        )

    logger.info("ocean-data-parser[{}]: log-level={}", __version__, log_level)
    if show_arguments:
        click.echo("odpy parameters inputs:")
        click.echo(f"verbose={verbose}")
        click.echo(f"log_level={log_level}")
        click.echo(f"log_file={log_file}")
        click.echo(f"log_file_level={log_file_level}")


main.add_command(convert.cli)
main.add_command(inspect_variables)

if __name__ == "__main__":
    main(auto_envar_prefix="ODPY")
