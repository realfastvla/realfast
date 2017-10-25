from __future__ import print_function, division, absolute_import  #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input  #, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import pickle
import os.path
import glob
import shutil
import random
from astropy import time
from evla_mcast.controller import Controller
from rfpipe import state, preferences
from realfast import pipeline, elastic, sdm_builder

import logging
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
ch.setFormatter(formatter)
logger = logging.getLogger('realfast_controller')

_vys_cfile = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'
_preffile = '/lustre/evla/test/realfast/realfast.yml'
_vys_timeout = 10  # seconds more than segment length
_distributed_host = 'cbe-node-01'

mock_standards = [(0.1, 30, 20, 0.01, 1e-3, 1e-3),
                  (0.1, 30, 20, 0.01, -1e-3, 1e-3),
                  (0.1, 30, 20, 0.01, -1e-3, -1e-3),
                  (0.1, 30, 20, 0.01, 1e-3, -1e-3)]  # (amp, i0, dm, dt, l, m)


class realfast_controller(Controller):

    def __init__(self, preffile=_preffile, inprefs={},
                 vys_timeout=_vys_timeout, datasource=None, tags=None,
                 mockprob=0.5, saveproducts=False, indexresults=True,
                 archiveproducts=False):
        """ Creates controller object that can act on a scan configuration.
        Inherits a "run" method that starts asynchronous operation.
        datasource of None defaults to "vys" or "sdm", by sim" is an option.
        tags is a default string for candidates put into index (None -> "new").
        mockprob is a prob (range 0-1) that a mock is added to each segment.
        """

        super(realfast_controller, self).__init__()
        self.preffile = preffile
        self.inprefs = inprefs
        self.vys_timeout = vys_timeout
        self.futures = {}
        self.datasource = datasource
        self.tags = tags
        self.mockprob = mockprob
        self.client = None
        self.indexresults = indexresults
        self.saveproducts = saveproducts
        self.archiveproducts = archiveproducts

        # TODO: add yaml parsing to overload via self.preffile['realfast']?

    def __repr__(self):
        return ('realfast controller with {0} jobs'
                .format(len(self.futures)))

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        summarize(config)

        if self.datasource is None:
            self.datasource = 'vys'

        self.inject_transient(config.scanId)  # randomly inject mock transient

        if runsearch(config):
            logger.info('Config looks good. Generating rfpipe state...')
            st = state.State(config=config, preffile=self.preffile,
                             inprefs=self.inprefs,
                             inmeta={'datasource': self.datasource})
            if self.indexresults:
                elastic.indexscan_config(config, preferences=st.prefs,
                                         datasource=self.datasource)
            else:
                logger.info("Not indexing config or prefs.")

            logger.info('Starting pipeline...')
            # pipeline returns dict of futures
            # TODO: update for dict structure
            futures = pipeline.pipeline_scan(st, segments=None,
                                             host=_distributed_host,
                                             cfile=_vys_cfile,
                                             vys_timeout=self.vys_timeout)
            self.futures[config.scanId] = futures
            self.client = futures[0]['data'].client
        else:
            logger.info("Config not suitable for realfast. Skipping.")

        # end of job clean up (indexing and removing from job list)
        self.cleanup()
        # TODO: this only runs when new data arrives. how to run at end/ctrl-c?

    def handle_sdm(self, sdmfile, sdmscan, bdfdir=None):
        """ Parallel to handle_config, but allows sdm to be passed in.
        Gets called explicitly. No cleanup done.
        """

        if self.datasource is None:
            self.datasource = 'sdm'

        # TODO: subscan assumed = 1
        subscan = 1
        scanId = '{0}.{1}.{2}'.format([os.path.basename(sdmfile.rstrip('/')),
                                       str(sdmscan), str(subscan)])
        self.inject_transient(scanId)  # randomly inject mock transient

        st = state.State(sdmfile=sdmfile, sdmscan=sdmscan, bdfdir=bdfdir,
                         preffile=self.preffile, inprefs=self.inprefs,
                         inmeta={'datasource': self.datasource})

        if self.indexresults:
            elastic.indexscan_sdm(scanId, preferences=st.prefs,
                                  datasource=self.datasource)
        else:
            logger.info("Not indexing sdm scan or prefs.")

        logger.info('Starting pipeline...')
        # pipeline returns state object per DM/dt
        futures = pipeline.pipeline_scan(st, segments=None,
                                         host=_distributed_host)

        self.futures[scanId] = futures
        self.client = futures[0]['data'].client

    def handle_finish(self, dataset):
        """ Triggered when obs doc defines end of a script.
        """

        logger.info('End of scheduling block message received.')

    def cleanup(self):
        """ Scan job dict, remove finished jobs,
        and push results to relevant indices.
        """

        removed = 0
        cindexed = 0
        sdms = 0

        for scanId in self.futures:
            logger.info("Checking on jobs from scanId {0}".format(scanId))

            # create list of futures (a dict per segment) that are done
            removelist = [futures for futurelist in self.futures[scanId]
                          for futures in futurelist
                          if futures['candcollection'].status in ['finished',
                                                                  'cancelled']]

            # one canddf per segment
            for futures in removelist:
                candcollection = futures['candcollection'].result()
                data = futures['data'].result()

                if len(candcollection.array):
# TODO: can we use client here?
#                    res = self.client.submit(elastic.indexcands, job, scanId, tags=self.tags)
                    if self.indexresults:
                        res = elastic.indexcands(candcollection.array, scanId,
                                                 prefsname=candcollection.prefs.name,
                                                 tags=self.tags)
                        cindexed += res
                    else:
                       logger.info("Not indexing cands.")

                else:
                    logger.info('No candidates for scanId {0}, scan {1} and '
                                'segment {2}.'.format(scanId,
                                                      candcollection.scan,
                                                      candcollection.segment))

# TODO: index noises
#                if os.path.exists(st.noisefile):
#                   if self.indexresults:
#                       res = elastic.indexnoises(st.noisefile, scanId)
#                        nindexed += res
#                   else:
#                      logger.info("Not indexing noises.")
#                else:
#                    logger.info('No noisefile found, no noises indexed.')

                # remove job from list
                self.futures[scanId].remove(futures)
                removed += 1

                # for last job of scanId trigger further cleanup
                if len(self.futures[scanId]) == 0:
                    _ = self.futures.pop(scanId)
                    if self.saveproducts:
                        candplots = moveplots(candcollection.prefs.workdir,
                                              scanId)
                        if len(candplots):
                            logger.info('Candidate plots copied from {0}'
                                        .format(candplots))
                        else:
                            logger.info('No candidate plots found to copy.')

                        newsdms = createproducts(candcollection, data)
                        if len(newsdms):
                            logger.info("Created new SDMs at: {0}"
                                        .format(newsdms))
                        else:
                            logger.info("No new SDMs created")

                        if self.archiveproducts:
                            archiveproducts(newsdms)

                    else:
                        logger.info("Not creating new SDMs or moving cand plots.")

        if removed:
            logger.info('Removed {0} jobs, indexed {1} cands, made {2} SDMs.'
                        .format(removed, cindexed, sdms))

    def inject_transient(self, scanId):
        """ Randomly sets preferences for scan to injects a transient
        into each segment.
        Also pushes mock properties to index.
        """

        if random.uniform(0, 1) < self.mockprob:
            mockparams = random.choice(mock_standards)
            self.inprefs['simulated_transient'] = [mockparams]
# TODO: indexmocks function
#           if self.indexresults:
#               mindexed = elastic.indexmocks(self.inprefs, scanId)
#                logger.info("Indexed {0} mock transients.".format(mindexed))
#            else:
#                logger.info("Not indexing mocks.")

            if self.tags is None:
                self.tags = 'mock'
            elif 'mock' not in self.tags:
                self.tags = ','.join(self.tags.split(',') + ['mock'])
        elif self.tags is not None:
            if 'mock' in self.tags:
                self.tags = ','.join(self.tags.split(',').remove('mock'))

    @property
    def statuses(self):
        return ['{0}, {1}: {2}'.format(scanId, ftype,
                                       futures[ftype].status)
                for (scanId, futurelist) in iteritems(self.futures)
                for futures in futurelist
                for ftype in futures]

    @property
    def errors(self):
        return ['{0}, {1}: {2}'.format(scanId, ftype,
                                       futures[ftype].exception())
                for (scanId, futurelist) in iteritems(self.futures)
                for futures in futurelist
                for ftype in futures
                if futures[ftype].status == 'error']


def runsearch(config):
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
        logger.warn("Scan startTime and stopTime are in the past ({0}, {1} < {2})"
                    .format(startTime, stopTime, now))
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


def createproducts(candcollection, data, bdfdir='.'):
    """ Create SDMs and BDFs for a given candcollection (time segment).
    """

    if len(candcollection.array) == 0:
        logger.info('No candidates to generate products for.')
        return []

    metadata = candcollection.metadata
    ninttot, nbl, nchantot, npol = data.shape
    nspw = len(metadata.spworder)
    nchan = nchantot//nspw
    segment = candcollection.segment
    if not isinstance(segment, int):
        logger.warn("Cannot get unique segment from candcollection ({0})".format(segment))
    st = candcollection.getstate()

    sdmlocs = []
    candranges = gencandranges(candcollection)
    for (startTime, endTime) in candranges:
        i = (86400*(startTime-st.segmenttimes[segment][0])/metadata.inttime).astype(int)
        nint = (86400*(endTime-startTime)/metadata.inttime).astype(int)  # TODO: may be off by 1
        data_cut = data[i:i+nint].reshape(nint, nbl, nspw, 1, nchan, npol)

        sdmloc = sdm_builder.makesdm(startTime, endTime, metadata, data_cut)
        sdmlocs.append(sdmloc)
        sdm_builder.makebdf(startTime, endTime, metadata, data_cut, bdfdir=bdfdir)

    return sdmlocs


def gencandranges(candcollection):
    """ Given a candcollection, define a list of candidate time ranges.
    """

    segment = candcollection.segment
    st = candcollection.getstate()

    # save whole segment
    return [(st.segmenttimes[segment][0], st.segmenttimes[segment][1])]


def archiveproducts(sdms):
    """ Call archive tool or move data to trigger archiving of sdms.
    """

    NotImplementedError


def moveplots(workdir, scanId,
              destination='/users/claw/public_html/realfast/plots'):
    """ For given fileroot, move candidate plots to public location
    """

    datasetId, scan, subscan = scanId.rsplit('.', 2)

    candplots = glob.glob('{0}/cands_{1}*.png'.format(workdir, datasetId))
    for candplot in candplots:
        shutil.copy(candplot, destination)

    return candplots


class config_controller(Controller):

    def __init__(self, pklfile=None, preffile=_preffile):
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
            prefs = preferences.Preferences(**preferences.parsepreffile(self.preffile))
            elastic.indexscan_config(config, preferences=prefs)
