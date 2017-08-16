from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

from evla_mcast.controller import Controller
import rfpipe

import logging
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
ch.setFormatter(formatter)
logger = logging.getLogger('rfcontroller')
logger.setLevel(logging.INFO)

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
        self.jobs = []

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        logger.info('Received complete configuration for {0}, '
                    'scan {1}, source {2}, intent {3}'
                    .format(config.scanId, config.scanNo, config.source,
                            config.scan_intent))

        if self.runsearch(config):
            logger.info('Config looks good. Generating rfpipe state...')
            try:
                st = rfpipe.state.State(config=config, preffile=self.preffile,
                                        inprefs=self.inprefs)
            except KeyError as exc:
                logger.warn('KeyError in parsing VCI? {0}'.format(exc))
            else:
                logger.info('Starting pipeline...')
                jobs = rfpipe.pipeline.pipeline_scan_distributed(st,
                                                                 host=distributed_host,
                                                                 cfile=vys_cfile,
                                                                 vys_timeout=self.vys_timeout)
                self.jobs += jobs

        else:
            logger.info("Config not suitable for realfast. Skipping.")

        removed = 0
        for job in self.jobs:
            if job.status == 'finished':
                _ = self.jobs.remove(job)
                removed += 1
        if removed:
            logger.info('Removed {0} finished jobs from job queue.'.format(removed))

    def handle_finish(self, dataset):
        """ Triggered when obs doc defines end of a script.
        """

        logger.info('End of scheduling block message received.')

    def runsearch(self, config):
        """ Test whether configuration specifies a config that realfast should search
        """

        # find config properties of interest
        intent = config.scan_intent
        antennas = config.get_antennas()
        antnames = [str(ant.name) for ant in antennas]
        subbands = config.get_subbands()
        inttimes = [subband.hw_time_res for subband in subbands]
        pols = [subband.pp for subband in subbands]
        nchans = [subband.spectralChannels for subband in subbands]
        chansizes = [subband.bw/subband.spectralChannels for subband in subbands]
        reffreqs = [subband.sky_center_freq*1e6 for subband in subbands]

        # Do not process if...
        # 1) chansize changes between subbands
        if not all([chansizes[0] == chansize for chansize in chansizes]):
            logger.warn("Channel size changes between subbands: {0}"
                        .format(chansizes))
            return False

        return True
