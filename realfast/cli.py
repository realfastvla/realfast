from realfast import controllers
import rfpipe
import click, os.path

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.captureWarnings(True)
logger = logging.getLogger(__name__)

vys_cfile = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'
default_preffile = '/lustre/evla/test/realfast/realfast.yml'
default_vys_timeout = 10  # seconds more than segment length
distributed_host = 'cbe-node-01'

@click.group('realfast')
def cli():
    pass


@cli.command()
@click.option('--pklfile')
@click.option('--index')
def config_catcher(pklfile, index):
    """ Runs async process to catch multicast messages to form scan config objects
    Can be saved to pklfile or elasticsearch index.
    """

    logger.info("Running config catcher with pklfile={0} and index={1}"
                .format(pklfile, index))
    config = controllers.config_controller(pklfile=pklfile, index=index)
    config.run()


@cli.command()
@click.option('--preffile', default=default_preffile)
@click.option('--vys_timeout', default=default_vys_timeout)
def run(preffile, vys_timeout):
    """ Run realfast controller to catch scan configs and start rfpipe.
    """

    rfc = controllers.realfast_controller(preffile=preffile,
                                          vys_timeout=vys_timeout)
    rfc.run()
