from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import pickle
from time import sleep
from astropy import time
from evla_mcast.controller import Controller
import rfpipe
from realfast import elastic

import logging
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
ch.setFormatter(formatter)
logger = logging.getLogger('realfast_controller')

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

        self.summarize(config)

        if self.runsearch(config):
            logger.info('Config looks good. Generating rfpipe state...')
            st = rfpipe.state.State(config=config, preffile=self.preffile,
                                    inprefs=self.inprefs)
            logger.info('Starting pipeline...')
            jobs = rfpipe.pipeline.pipeline_scan_distributed(st, segments=None,
                                                             host=distributed_host,
                                                             cfile=vys_cfile,
                                                             vys_timeout=self.vys_timeout)
            self.jobs += jobs
#            rfpipe.pipeline.pipeline_seg2(st, 0, preffile=self.preffile,
#                                          inprefs=self.inprefs)
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

        # 2) start and stop time is after current time
        now = time.Time.now().unix
        startTime = time.Time(config.startTime, format='mjd').unix
        stopTime = time.Time(config.stopTime, format='mjd').unix
        if (startTime > now) and (stopTime > now):
            logger.warn("Scan startTime and stopTime are in the past ({0}, {1} < {2})".format(startTime, stopTime, now))
            return False

        return True

    def summarize(self, config):
        """ Print summary info for config
        """

        try:
            logger.info(':: ConfigID {0} ::'.format(config.configId))
            logger.info('\tScan {0}, source {1}, intent {2}'
                        .format(config.scanNo, config.source,
                                config.scan_intent))
            logger.info('\t(RA, Dec) = ({0}, {1})'
                        .format(config.ra_deg, config.dec_deg))
            subbands = config.get_subbands()

            reffreqs = [subband.sky_center_freq for subband in subbands]
            logger.info('\tFreq: {0} - {1}'
                        .format(min(reffreqs), max(reffreqs)))

            nchans = [subband.spectralChannels for subband in subbands]
            chansizes = [subband.bw/subband.spectralChannels
                         for subband in subbands]
            sb0 = subbands[0]
            logger.info('\t(nspw, chan/spw, nchan) = ({0}, {1}, {2})'
                        .format(len(nchans), nchans[0], sum(nchans)))
            logger.info('\t(BW, chansize) = ({0}, {1}) MHz'
                        .format(sb0.bw, chansizes[0]))
            if not all([chansizes[0] == chansize for chansize in chansizes]):
                logger.info('\tNot all spw have same configuration.')

            logger.info('\t(nant, npol) = ({0}, {1})'
                        .format(config.numAntenna, sb0.npp))
            dt = 24*3600*(config.stopTime-config.startTime)
            logger.info('\t(StartMJD, duration) = ({0}, {1}s).'
                        .format(config.startTime, round(dt, 1)))
            logger.info('\t({0}/{1}) ints at (HW/Final) integration time of ({2}/{3}) s'
                        .format(int(round(dt/sb0.hw_time_res)),
                                int(round(dt/sb0.final_time_res)),
                                sb0.hw_time_res, sb0.final_time_res))
        except:
            logger.warn("Failed to fully parse config to print summary."
                        "Proceeding.")

    @property
    def statuses(self):
        return [self.jobs[i].status for i in range(len(self.jobs))]

    @property
    def errors(self):
        return [self.jobs[i].exception() for i in range(len(self.jobs))
                if self.jobs[i].status == 'error']

    @property
    def client(self):
        if len(self.jobs):
            return self.jobs[0].client
        else:
            logger.warn('No job running. Cannot get client.')


class config_controller(Controller):

    def __init__(self, pklfile=None, preffile=None):
        """ Creates controller object that saves scan configs.
        If pklfile is defined, it will save pickle there.
        If preffile is defined, it will attach a preferences to indexed scan.
        Inherits a "run" method that starts asynchronous operation.
        """

        super(config_controller, self).__init__()
        self.pklfile = pklfile
        self.preffile = preffile

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        logger.info('Received complete configuration for {0}, '
                    'scan {1}, source {2}, intent {3}'
                    .format(config.scanId, config.scanNo, config.source,
                            config.scan_intent))

        if self.pklfile:
            with open(self.pklfile, 'ab') as pkl:
                pickle.dump(config, pkl)

        if self.preffile:
            prefs = rfpipe.preferences.Preferences(preffile=preffile)
            elastic.indexscan(config, preferences=prefs)
