import logging
import os
import sys

import click
from loguru import logger

from ocean_data_parser import __version__
from ocean_data_parser.batch.convert import convert as convert
from ocean_data_parser.compile.netcdf import compile as compile

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
    """
    This class is used to redirect logging loggers to loguru
    # https://stackoverflow.com/questions/66769431/how-to-use-loguru-with-standard-loggers
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


@click.group(name="odpy")
@click.version_option(__version__)
@click.option(
    "--verbose",
    is_flag=True,
    help=f"Logger format present more information",
    default=False,
)
@click.option(
    "--log-level",
    type=click.Choice(LOG_LEVELS),
    help="Logger level used",
    default="INFO",
)
@click.option("--log-file", type=click.Path(), help="Log output to a file.")
@click.option(
    "--log-file-level",
    type=click.Choice(LOG_LEVELS),
    help="File ogger level used",
    default="INFO",
)
def main(verbose, log_level, log_file, log_file_level):
    log_format = VERBOSE_LOG_FORMAT if verbose else LOG_FORMAT
    logger.add(
        sys.stderr,
        level=log_level,
        format=VERBOSE_LOG_FORMAT if verbose else LOG_FORMAT,
    )
    if log_file:
        logger.add(log_file, level=log_file_level, format=log_format)


main.add_command(convert)
main.add_command(compile)