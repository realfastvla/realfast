from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

from evla_mcast.controller import Controller
import rfpipe
import distributed

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

vys_cfile = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'
default_preffile = '/lustre/evla/test/realfast/realfast.yml'
default_vys_timeout = 10  # seconds more than segment length
distributed_host = 'cbe-node-01'


class realfast_controller(Controller):

    def __init__(self, preffile=default_preffile, inprefs={},
                 vys_timeout=default_vys_timeout):
        """ Creates controller object that can act on a scan configuration.
        Inherits a "run" method that starts asynchronous operation.
        """

        super(realfast_controller, self).__init__()
        self.preffile = preffile
        self.inprefs = inprefs
        self.vys_timeout = vys_timeout

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        logger.info('Received complete configuration for {0}, scan {1}'
                    .format(config.scanId, config.scanNo))

        logger.info('TODO: print more config info')

        try:
            logger.info('Generating rfpipe state...')
            st = rfpipe.state.State(config=config, preffile=self.preffile,
                                    inprefs=self.inprefs)

            logger.info('Starting pipeline...')
            rfpipe.pipeline.pipeline_scan_distributed(st, segments=[0],
                                                      host=distributed_host,
                                                      cfile=vys_cfile,
                                                      vys_timeout=self.vys_timeout)
        except KeyError as exc:
            logger.warn('KeyError in parsing VCI? {0}'.format(exc))

    def handle_finish(self, dataset):
        """ Triggered when obs doc defines end of a script.
        """

        logger.info('End of scheduling block message received')
