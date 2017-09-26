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
    filename defines pickle file to save objects in.
    """

    config = controllers.config_controller(filename)
    config.run()

