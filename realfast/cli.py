from realfast import controllers
import rfpipe
import click, os.path

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.captureWarnings(True)
logger = logging.getLogger(__name__)


@click.group('realfast')
def cli():
    pass


@cli.command()
@click.argument('filename')
def config_catcher(filename):
    """ Runs async process to catch multicast messages to form scan config objects
    filename defines pickle file in which complete scan configs are saved.
    """

    config = controllers.config_controller(filename)
    config.run()


@cli.command()
@click.option('--preffile')
@click.option('--vys_timeout')
def run(preffile, vys_timeout):
    """ Run realfast controller to catch scan configs and start rfpipe.
    """

    rfc = controllers.realfast_controller(preffile=preffile, vys_timeout=vys_timeout)
    rfc.run()
