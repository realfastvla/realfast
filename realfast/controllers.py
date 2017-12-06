from __future__ import print_function, division, absolute_import  #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input  #, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import pickle
import os.path
import glob
import shutil
import random
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg
import numpy as np
from astropy import time
import dask.utils
from evla_mcast.controller import Controller
from rfpipe import state, preferences, candidates
from realfast import pipeline, elastic, sdm_builder

import logging
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
ch.setFormatter(formatter)
logger = logging.getLogger('realfast_controller')

_vys_cfile = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'
_preffile = '/lustre/evla/test/realfast/realfast.yml'
_vys_timeout = 30  # scale wait by realtime
_distributed_host = 'cbe-node-01'
_candplot_dir = '/users/claw/public_html/realfast/plots'
_candplot_url_prefix = 'http://www.aoc.nrao.edu/~claw/realfast/plots'

# standards should always have l=0 or m=0
# list of (seg, i0, dm, dt, amp, l, m)
_mock_standards = [(0, 1, 20, 0.01, 0.1, 1e-3, 0.),
                   (0, 1, 20, 0.01, 0.1, -1e-3, 0.),
                   (0, 1, 20, 0.01, 0.1, 0., 1e-3),
                   (0, 1, 20, 0.01, 0.1, 0., -1e-3)]


class realfast_controller(Controller):

    def __init__(self, preffile=_preffile, inprefs={},
                 vys_timeout=_vys_timeout, datasource=None, tags=None,
                 mockprob=0.5, mockset=_mock_standards, saveproducts=False,
                 indexresults=True, archiveproducts=False, nameincludes=None,
                 searchintents=['OBSERVE_TARGET', 'CALIBRATE_PHASE',
                                'CALIBRATE_AMPLI', 'CALIBRATE_DELAY']):
        """ Creates controller object that can act on a scan configuration.
        Inherits a "run" method that starts asynchronous operation.
        datasource of None defaults to "vys" or "sdm", by sim" is an option.
        tags is comma-delimited string for cands put in index (None -> "new").
        mockprob is a prob (range 0-1) that a mock is added to each segment.
        nameincludes is a string required to be in datasetId.
        searchintents is a list of intent names to search.
        """

        # TODO: add argument for selecting by datasetId?
        super(realfast_controller, self).__init__()
        self.preffile = preffile
        self.inprefs = inprefs
        self.vys_timeout = vys_timeout
        self.states = {}
        self.futures = {}
        self.datasource = datasource
        self.tags = tags
        self.mockprob = mockprob
        self.mockset = mockset
        self.client = None
        self.indexresults = indexresults
        self.saveproducts = saveproducts
        self.archiveproducts = archiveproducts
        self.nameincludes = nameincludes
        self.searchintents = searchintents
        self.lock = dask.utils.SerializableLock()

        # TODO: add yaml parsing to overload via self.preffile['realfast']?

    def __repr__(self):
        return ('realfast controller with {0} jobs'
                .format(len(self.futures)))

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

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        summarize(config)

        if self.datasource is None:
            self.datasource = 'vys'

        self.inject_transient(config.scanId)  # randomly inject mock transient

        if runsearch(config, nameincludes=self.nameincludes,
                     searchintents=self.searchintents):
            logger.info('Config looks good. Generating rfpipe state...')
            st = state.State(config=config, preffile=self.preffile,
                             inprefs=self.inprefs, lock=self.lock,
                             inmeta={'datasource': self.datasource})
            if self.indexresults:
                elastic.indexscan_config(config, preferences=st.prefs,
                                         datasource=self.datasource)
            else:
                logger.info("Not indexing config or prefs.")

            logger.info('Starting pipeline...')
            # pipeline returns dict of futures
            # ** is list of dicts required to be in segment order?
            futures = pipeline.pipeline_scan(st, segments=None,
                                             host=_distributed_host,
                                             cfile=_vys_cfile,
                                             vys_timeout=self.vys_timeout)
            self.futures[config.scanId] = futures
            self.states[config.scanId] = st
            self.client = futures[0]['candcollection'].client
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
        sdmsubscan = 1
        scanId = '{0}.{1}.{2}'.format(os.path.basename(sdmfile.rstrip('/')),
                                      str(sdmscan), str(sdmsubscan))
        self.inject_transient(scanId)  # randomly inject mock transient

        st = state.State(sdmfile=sdmfile, sdmscan=sdmscan, bdfdir=bdfdir,
                         preffile=self.preffile, inprefs=self.inprefs,
                         lock=self.lock,
                         inmeta={'datasource': self.datasource})

        if self.indexresults:
            elastic.indexscan_sdm(sdmfile, sdmscan, sdmsubscan,
                                  preferences=st.prefs,
                                  datasource=self.datasource)
        else:
            logger.info("Not indexing sdm scan or prefs.")

        logger.info('Starting pipeline...')
        # pipeline returns state object per DM/dt
        futures = pipeline.pipeline_scan(st, segments=None,
                                         host=_distributed_host)

        self.futures[scanId] = futures
        self.states[scanId] = st
        self.client = futures[0]['candcollection'].client

    def handle_meta(self, inmeta):
        """ Parallel to handle_config, but allows metadata dict to be passed in.
        Gets called explicitly. No cleanup done.
        """

        if self.datasource is None:
            self.datasource = 'vys'

        st = state.State(preffile=self.preffile, inprefs=self.inprefs,
                         inmeta=inmeta, lock=self.lock)

        logger.info('Starting pipeline...')
        # pipeline returns state object per DM/dt
        futures = pipeline.pipeline_scan(st, segments=None,
                                         host=_distributed_host,
                                         cfile=_vys_cfile,
                                         vys_timeout=self.vys_timeout)

        self.futures[st.metadata.scanId] = futures
        self.states[st.metadata.scanId] = st
        self.client = futures[0]['candcollection'].client

    def handle_finish(self, dataset):
        """ Triggered when obs doc defines end of a script.
        """

        logger.info('End of scheduling block message received.')

    def cleanup(self, badstatuslist=['cancelled']):
        """ Scan job dict, remove finished jobs,
        and push results to relevant indices.
        badstatuslist can include 'cancelled', 'error', 'lost'.
        """

        removed = 0
        cindexed = 0
        sdms = 0

        for status in badstatuslist:
            removed += self.removefutures(status)

        for scanId in self.futures:
            logger.info("Checking on jobs from scanId {0}".format(scanId))

            # create list of futures (a dict per segment) that are done
            finishedlist = [futures for (scanId0, futurelist) in iteritems(self.futures)
                            for futures in futurelist
                            if (futures['candcollection'].status == 'finished') and
                               (scanId0 == scanId)]

            # one candcollection per segment
            for futures in finishedlist:
                candcollection = futures['candcollection'].result()
                if len(candcollection.array):
                    if self.indexresults:
                        nplots = moveplots(candcollection.prefs.workdir,
                                           scanId, destination=_candplot_dir)
#                       TODO: can we use client here?
                        res = elastic.indexcands(candcollection, scanId,
                                                 tags=self.tags,
                                                 url_prefix=_candplot_url_prefix)
                        if res or nplots:
                            logger.info('Indexed {0} candidates and moved {1} '
                                        'plots to {2}'
                                        .format(res, nplots, _candplot_dir))
                        else:
                            logger.info('No candidates or plots found.')

                        cindexed += res
                    else:
                        logger.info("Not indexing cands.")

                    makesummaryplot(candcollection.prefs.workdir, scanId)

                else:
                    logger.info('No candidates for a segment from scanId {0}'
                                .format(scanId))

# TODO: index noises
#                if os.path.exists(st.noisefile):
#                   if self.indexresults:
#                       res = elastic.indexnoises(st.noisefile, scanId)
#                        nindexed += res
#                   else:
#                      logger.info("Not indexing noises.")
#                else:
#                    logger.info('No noisefile found, no noises indexed.')

                # optionally save and archive sdm/bdfs for segment
                if self.saveproducts:
                    newsdms = createproducts(candcollection, futures['data'])
                    if len(newsdms):
                        logger.info("Created new SDMs at: {0}"
                                    .format(newsdms))
                    else:
                        logger.info("No new SDMs created")

                    if self.archiveproducts:
                        runingest(newsdms)  # TODO: implement this

                else:
                    logger.info("Not making new SDMs or moving candplots.")

                # remove job from list
                self.futures[scanId].remove(futures)
                removed += 1

        # after scanId loop, clean up self.futures
        removeids = [scanId for scanId in self.futures
                     if len(self.futures[scanId]) == 0]
        for scanId in removeids:
            _ = self.futures.pop(scanId)
            _ = self.states.pop(scanId)

        if removed:
            logger.info('Removed {0} jobs, indexed {1} cands, made {2} SDMs.'
                        .format(removed, cindexed, sdms))

    def inject_transient(self, scanId):
        """ Randomly sets preferences for scan to injects a transient
        into each segment.
        Also pushes mock properties to index.
        """

        if random.uniform(0, 1) < self.mockprob:
            mockparams = random.choice(self.mockset)
            self.inprefs['simulated_transient'] = [mockparams]

            if self.indexresults:
                mindexed = elastic.indexmocks(self.inprefs, scanId)
                logger.info("Indexed {0} mock transients.".format(mindexed))
            else:
                logger.info("Not indexing mocks.")

#            if self.tags is None:
#                self.tags = ['mock']
#            elif 'mock' not in self.tags:
#                self.tags = self.tags.append('mock')
#        elif self.tags is not None:
#            if 'mock' in self.tags:
#                self.tags = self.tags.remove('mock')

    def removefutures(self, status):
        """ Remove jobs with status of 'cancelled'
        """

        removed = 0
        for scanId in self.futures:
            logger.info("Checking on jobs from scanId {0}".format(scanId))

            # create list of futures (a dict per segment) that are cancelled
            removelist = [futures for (scanId0, futurelist) in iteritems(self.futures)
                             for futures in futurelist
                             if (futures['candcollection'].status == status or
                                 futures['data'].status == status) and
                                (scanId0 == scanId)]

            # clean them up
            for futures in removelist:
                self.futures[scanId].remove(futures)
                removed += 1

        return removed


def runsearch(config, nameincludes=None, searchintents=None):
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
    if (startTime < now) and (stopTime < now):
        logger.warn("Scan startTime and stopTime are in the past ({0}, {1} < {2})"
                    .format(startTime, stopTime, now))
        return False

    # 3) if nameincludes set, reject if datasetId does not have it
    if nameincludes is not None:
        if nameincludes not in config.datasetId:
            logger.warn("datasetId {0} does not include nameincludes {1}"
                        .format(config.datasetId, nameincludes))
            return False

    # 4) only search if in searchintents
    if searchintents is not None:
        if not any([searchintent in intent for searchintent in searchintents]):
            logger.warn("intent {0} not in searchintents list {1}"
                        .format(intent, searchintents))
            return False

    return True


def summarize(config):
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


def createproducts(candcollection, datafuture, sdmdir='.',
                   bdfdir='/lustre/evla/wcbe/data/no_archive/'):
    """ Create SDMs and BDFs for a given candcollection (time segment).
    Takes data future and calls data only if windows found to cut.
    This uses the sdm_builder module, which calls the sdm builder server.
    sdmdir will move and rename output file to "realfast_<obsid>_<uid>".
    Currently BDFs are moved to no_archive lustre area by default.
    """

    if len(candcollection.array) == 0:
        logger.info('No candidates to generate products for.')
        return []

    metadata = candcollection.metadata
    nspw = len(metadata.spworder)
    nchan = nchantot//nspw
    segment = candcollection.segment
    if not isinstance(segment, int):
        logger.warn("Cannot get unique segment from candcollection ({0})".format(segment))
    st = candcollection.getstate()

    sdmlocs = []
    candranges = gencandranges(candcollection)  # finds time windows to save from segment
    if len(candcollection):
        data = datafuture.result()
        ninttot, nbl, nchantot, npol = data.shape
    else:
        logger.info('No candidate time ranges. Not calling for data.')

    for (startTime, endTime) in candranges:
        i = (86400*(startTime-st.segmenttimes[segment][0])/metadata.inttime).astype(int)
        nint = (86400*(endTime-startTime)/metadata.inttime).astype(int)  # TODO: may be off by 1
        data_cut = data[i:i+nint].reshape(nint, nbl, nspw, 1, nchan, npol)

        sdmloc = sdm_builder.makesdm(startTime, endTime, metadata, data_cut)
        if sdmloc is not None:
            if sdmdir is not None:
                uid = ('uid:///evla/realfastbdf/{0}'
                       .format(int(time.Time(startTime,
                                             format='mjd').unix*1e3)))
                if 'sdm-builder' in sdmloc:  # remove default name suffix
                    sdmlocbase = '-'.join(sdmloc.rsplit('-')[:-3])
                else:
                    sdmlocbase = sdmloc
                newsdmloc = os.path.join(sdmdir, 'realfast_{0}_{1}'
                                         .format(os.path.basename(sdmlocbase), uid.rsplit('/')[-1]))
                shutil.move(sdmloc, newsdmloc)
                sdmlocs.append(newsdmloc)
            else:
                sdmlocs.append(sdmloc)

            # TODO: migrate bdfdir to newsdmloc once ingest tool is ready
            sdm_builder.makebdf(startTime, endTime, metadata, data_cut,
                                bdfdir=bdfdir)
        else:
            logger.warn("No sdm/bdf made for start/end time {0}-{1}"
                        .format(startTime, endTime))

    return sdmlocs


def gencandranges(candcollection):
    """ Given a candcollection, define a list of candidate time ranges.
    """

    segment = candcollection.segment
    st = candcollection.getstate()

    # save whole segment
    return [(st.segmenttimes[segment][0], st.segmenttimes[segment][1])]


def runingest(sdms):
    """ Call archive tool or move data to trigger archiving of sdms.
    """

    NotImplementedError
#    /users/vlapipe/workflows/test/bin/ingest -m -p /home/mctest/evla/mcaf/workspace --file 


def moveplots(workdir, scanId, destination=_candplot_dir):
    """ For given fileroot, move candidate plots to public location
    """

    datasetId, scan, subscan = scanId.rsplit('.', 2)

    nplots = 0
    candplots = glob.glob('{0}/cands_{1}_*.png'.format(workdir, datasetId))
    for candplot in candplots:
        try:
            shutil.move(candplot, destination)
            nplots += 1
        except shutil.Error:
            logger.warn("Plot {0} already exists at {1}. Skipping..."
                        .format(candplot, destination))

    candplots = glob.glob('{0}/cands_{1}_*.png'.format(workdir, datasetId))

    return nplots


def makesummaryplot(workdir, scanId, destination=_candplot_dir):
    """ Create summary plot for a given scanId and move it
    """

    candsfile = '{0}/cands_{1}.pkl'.format(workdir, scanId)
    summaryplot = '{0}/cands_{1}.png'.format(workdir, scanId)

    times = np.zeros(0, dtype=float)
    dms = np.zeros(0, dtype=float)
    snrs = np.zeros(0, dtype=float)
    ls = np.zeros(0, dtype=float)
    ms = np.zeros(0, dtype=float)
    for candcoll in candidates.iter_cands(candsfile):
        times = np.concatenate((times, candcoll.candmjd))
        dms = np.concatenate((dms, candcoll.canddm))
        snrs = np.concatenate((snrs, candcoll.array['snr1']))
        ls = np.concatenate((ls, candcoll.array['l1']))
        ms = np.concatenate((ms, candcoll.array['m1']))

    fig = plt.Figure(figsize=(8, 4))
    canvas = FigureCanvasAgg(fig)
    # DM-time
    ax = fig.add_subplot(1, 2, 1, facecolor='white')
    ax.scatter(times, dms, s=(snrs/(0.9*snrs.min()))**3, facecolors=None)
    ax.set_xlabel('Time (MJD)')
    ax.set_ylabel('DM (pc/cm3)')
    ax = fig.add_subplot(1, 2, 2, facecolor='white')
    ax.scatter(ls, ms, s=(snrs/(0.9*snrs.min()))**3, facecolors=None)
    ax.set_xlabel('l (rad)')
    ax.set_ylabel('m (rad)')

    canvas.print_figure(summaryplot)

    summaryplotdest = os.path.join(destination, os.path.basename(summaryplot))
    shutil.move(summaryplot, summaryplotdest)


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
