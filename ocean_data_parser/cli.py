import click

from ocean_data_parser import __version__

from ocean_data_parser.batch.convert import convert as convert
from ocean_data_parser.compile.netcdf import compile as compile

@click.group(name='odpy')
@click.version_option(__version__)
def main():
    pass

main.add_command(convert)
main.add_command(compile)