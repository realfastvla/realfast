from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

from realfast import controllers
import click

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
@click.option('--preffile')
def config_catcher(pklfile, preffile):
    """ Runs async process to catch multicast messages to form scan config objects
    Can be saved to pklfile and optionally attached to preferences from preffile.
    """

    logger.info("Running config catcher with pklfile={0} and preffile={1}"
                .format(pklfile, preffile))
    config = controllers.config_controller(pklfile=pklfile, preffile=preffile)
    config.run()


@cli.command()
@click.option('--preffile', default=default_preffile)
@click.option('--vys_timeout', default=default_vys_timeout)
def run(preffile, vys_timeout):
    """ Run realfast controller to catch scan configs and start rfpipe.
    """

    logger.warn("Command line realfast tool will lose track of jobs and do " 
                "no indexing after ctrl-c.")
    rfc = controllers.realfast_controller(preffile=preffile,
                                          vys_timeout=vys_timeout)
    rfc.run()
