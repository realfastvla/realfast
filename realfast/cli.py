from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

from realfast import controllers, elastic
import click

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.captureWarnings(True)
logger = logging.getLogger(__name__)

vys_cfile = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'
default_preffile = '/lustre/evla/test/realfast/realfast.yml'
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
def run(preffile):
    """ Run realfast controller to catch scan configs and start rfpipe.
    """

    try:
        rfc = controllers.realfast_controller(preffile=preffile)
        rfc.run()
    except KeyboardInterrupt:
        logger.warn("Cleaning up before stopping processing.")
    finally:
        rfc.cleanup_loop()


@click.group('realfast_portal')
def cli2():
    pass


@cli2.command()
@click.argument('index')
def get_ids(index):
    """ Get ids in given index
    """

    logger.info("Getting Ids in index {0}".format(index))
    elastic.get_ids(index)


@cli2.command()
@click.option('--prefix1', default='new')
@click.option('--prefix2', default='final')
@click.argument('datasetid')
def move_dataset(prefix1, prefix2, datasetid):
    """ Move datasetId from prefix1 to prefix2
    """

    elastic.move_dataset(prefix1, prefix2, datasetid)


@cli2.command()
@click.option('--prefix1', default='new')
@click.option('--prefix2', default='final')
def move_consensus(prefix1, prefix2):
    """ Use consensus to move candidates from 1 to 2
    """

    pass


@cli2.command()
@click.option('--prefix', default='new')
@click.argument('datasetid')
def remove_dataset(prefix, datasetid):
    """ Remove all data associated with given datasetid
    """

    elastic.move_dataset(prefix, None, datasetid)


@cli2.command()
@click.argument('prefix')
def audit_indexprefix(prefix):
    """ Audit all indices with given prefix
    """

    elastic.audit_indexprefix(prefix)


@cli2.command()
@click.argument('prefix')
def reset_indices(prefix):
    """ Reset all indices with given prefix
    """

    elastic.reset_indices(prefix)
