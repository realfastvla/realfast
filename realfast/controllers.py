from __future__ import print_function, division, absolute_import#, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import pickle
import os.path
import glob
import shutil
import random
import distributed
from astropy import time
from time import sleep
import dask.utils
from evla_mcast.controller import Controller
from rfpipe import state, preferences, candidates
from realfast import pipeline, elastic, sdm_builder

import logging
import matplotlib
import yaml
matplotlib.use('Agg')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
ch.setFormatter(formatter)
logger = logging.getLogger('realfast_controller')

_vys_cfile_prod = '/home/cbe-master/realfast/lustre_workdir/vys.conf'  # production file
_vys_cfile_test = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'  # test file
_preffile = '/lustre/evla/test/realfast/realfast.yml'
_distributed_host = '192.168.201.101'  # for ib0 on cbe-node-01
_candplot_dir = '/users/claw/public_html/realfast/plots'
_candplot_url_prefix = 'http://www.aoc.nrao.edu/~claw/realfast/plots'

# standards should always have l=0 or m=0
# list of (seg, i0, dm, dt, amp, l, m)
_mock_standards = [(0, 1, 20, 0.01, 0.1, 1e-3, 0.),
                   (0, 1, 20, 0.01, 0.1, -1e-3, 0.),
                   (0, 1, 20, 0.01, 0.1, 0., 1e-3),
                   (0, 1, 20, 0.01, 0.1, 0., -1e-3)]


class realfast_controller(Controller):

    def __init__(self, preffile=_preffile, inprefs={}, **kwargs):
        """ Creates controller object that can act on a scan configuration.
        Inherits a "run" method that starts asynchronous operation that calls
        handle_config. handle_sdm and handle_meta (with datasource "vys" or
        "sim") are also supported.

        kwargs can include:
        - tags, a comma-delimited string for cands to index (None -> "new"),
        - nameincludes, a string required to be in datasetId,
        - vys_timeout, factor over real-time for vys reading to wait,
        - mockprob, chance (range 0-1) that a mock is added,
        - saveproducts, boolean defining generation of mini-sdm,
        - indexresults, boolean defining push (meta)data to search index,
        - archiveproducts, boolean defining archiving mini-sdm,
        - throttle, boolean defining whether to slow pipeline submission,
        - read_overhead, throttle param requires multiple of vismem in a READERs memory,
        - read_totfrac, throttle param requires fraction of total READER memory be available,
        - searchintents, a list of intent names to search.
        """

        super(realfast_controller, self).__init__()

        self.inprefs = inprefs  # rfpipe preferences
        self.mockset = _mock_standards
        self.client = distributed.Client('{0}:{1}'
                                         .format(_distributed_host, '8786'))
        self.lock = dask.utils.SerializableLock()
        self.states = {}
        self.futures = {}
        self.futures_removed = {}

        # define attributes from yaml file
        prefs = {}
        self.preffile = preffile
        if self.preffile is not None:
            if os.path.exists(self.preffile):
                with open(self.preffile, 'r') as fp:
                    prefs = yaml.load(fp)['realfast']
                    logger.info("Parsed realfast preffile {0}"
                                .format(self.preffile))
            else:
                logger.warn("realfast preffile {0} given, but not found"
                            .format(self.preffile))
        else:
            logger.info("No realfast preffile provided.")

        # get arguments from preffile, optional overload from kwargs
        for attr in ['tags', 'nameincludes', 'mockprob', 'vys_timeout',
                     'indexresults', 'saveproducts', 'archiveproducts',
                     'searchintents', 'throttle', 'read_overhead',
                     'read_totfrac']:
            setattr(self, attr, None)
            if attr in prefs:
                setattr(self, attr, prefs[attr])
            if attr in kwargs:
                setattr(self, attr, kwargs[attr])

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

    def handle_config(self, config, cfile=_vys_cfile_prod):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        Default vys config file uses production parameters.
        """

        summarize(config)

        if 'simulated_transient' not in self.inprefs:
            self.inject_transient(config.scanId)  # randomly inject mock transient

        if runsearch(config, nameincludes=self.nameincludes,
                     searchintents=self.searchintents):
            logger.info('Config looks good. Generating rfpipe state...')

            self.set_state(config.scanId, config=config, inmeta={'datasource': 'vys'})

            if self.indexresults:
                elastic.indexscan_config(config,
                                         preferences=self.states[config.scanId].prefs,
                                         datasource='vys')
            else:
                logger.info("Not indexing config or prefs.")

            self.start_pipeline(config.scanId, cfile=cfile)

        else:
            logger.info("Config not suitable for realfast. Skipping.")

        # end of job clean up (indexing and removing from job list)
        self.cleanup()
        # TODO: this only runs when new data arrives. how to run at end/ctrl-c?

    def handle_sdm(self, sdmfile, sdmscan, bdfdir=None):
        """ Parallel to handle_config, but allows sdm to be passed in.
        Gets called explicitly. No cleanup done.
        """

        # TODO: subscan assumed = 1
        sdmsubscan = 1
        scanId = '{0}.{1}.{2}'.format(os.path.basename(sdmfile.rstrip('/')),
                                      str(sdmscan), str(sdmsubscan))

        if 'simulated_transient' not in self.inprefs:
            self.inject_transient(scanId)  # randomly inject mock transient

        self.set_state(scanId, sdmfile=sdmfile, sdmscan=sdmscan, bdfdir=bdfdir,
                       inmeta={'datasource': 'sdm'})

        if self.indexresults:
            elastic.indexscan_sdm(sdmfile, sdmscan, sdmsubscan,
                                  preferences=self.states[scanId].prefs,
                                  datasource='sdm')
        else:
            logger.info("Not indexing sdm scan or prefs.")

        self.start_pipeline(scanId)

        self.cleanup()

    def handle_meta(self, inmeta, cfile=_vys_cfile_test):
        """ Parallel to handle_config, but allows metadata dict to be passed in.
        Gets called explicitly.
        Default vys config file uses test parameters.
        inmeta datasource key can be 'vys' or 'sim' and is passed to rfpipe.
        """

        scanId = '{0}.{1}.{2}'.format(inmeta['datasetId'], str(inmeta['scan']),
                                      str(inmeta['subscan']))

        if 'simulated_transient' not in self.inprefs:
            self.inject_transient(scanId)  # randomly inject mock transient

        self.set_state(scanId, inmeta=inmeta)

        self.start_pipeline(scanId, cfile=cfile)

        self.cleanup()

    def set_state(self, scanId, config=None, inmeta=None, sdmfile=None,
                  sdmscan=None, bdfdir=None):
        """ Given metadata source, define state for a scanId.
        """

        st = state.State(inmeta=inmeta, config=config, preffile=self.preffile,
                         inprefs=self.inprefs, lock=self.lock,
                         sdmfile=sdmfile, sdmscan=sdmscan, bdfdir=bdfdir)

        self.states[scanId] = st

    def start_pipeline(self, scanId, cfile=None):
        """ Start pipeline conditional on cluster state.
        Sets futures and state after submission keyed by scanId.
        """

        st = self.states[scanId]

        # submit, with optional throttling
        futures = None
        if self.throttle and self.read_overhead and self.read_totfrac:
            w_memlim = self.read_overhead*st.vismem*1e9

            tot_memlim = self.read_totfrac*sum([v['memory_limit']
                                                for v in itervalues(self.client.scheduler_info()['workers'])
                                                if 'READER' in v['resources']])

            t0 = time.Time.now().unix
            timeout = st.metadata.inttime*st.metadata.nints
            elapsedtime = time.Time.now().unix - t0

            while (elapsedtime < timeout) and (futures is None):
                # Submit if workers are not overloaded
                if (worker_memory_ready(self.client, w_memlim) and
                   (total_memory_ready(self.client, tot_memlim))):
                    logger.info('Starting pipeline...')
                    futures = pipeline.pipeline_scan_delayed(st,
                                                             cl=self.client,
                                                             cfile=cfile,
                                                             vys_timeout=self.vys_timeout)
                else:
                    sleep(min(1, timeout/10))
                    self.cleanup()
                    elapsedtime = time.Time.now().unix - t0

            if elapsedtime > timeout:
                logger.info("Throttle timed out. ScanId {0} not submitted."
                            .format(st.metadata.scanId))

        else:
            logger.info('Starting pipeline...')
            futures = pipeline.pipeline_scan_delayed(st, cl=self.client,
                                                     cfile=cfile,
                                                     vys_timeout=self.vys_timeout)

        if futures is not None:
            self.futures[st.metadata.scanId] = futures

    def cleanup(self, badstatuslist=['cancelled', 'error', 'lost']):
        """ Scan job dict, remove finished jobs,
        and push results to relevant indices.
        badstatuslist can include 'cancelled', 'error', 'lost'.
        """

        removed = 0
        cindexed = 0
        sdms = 0

        scanIds = [scanId for scanId in self.futures]
        if len(scanIds):
            logger.info("Checking on jobs from {0} scanIds: {1}"
                        .format(len(scanIds), scanIds))

        for scanId in self.futures:
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
                    logger.debug('No candidates for a segment from scanId {0}'
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
                if self.saveproducts and len(candcollection):
                    newsdms = createproducts(candcollection, futures['data'])
                    if len(newsdms):
                        logger.info("Created new SDMs at: {0}"
                                    .format(newsdms))
                    else:
                        logger.info("No new SDMs created")

                    if self.archiveproducts:
                        runingest(newsdms)  # TODO: implement this

                else:
                    logger.debug("Not making new SDMs or moving candplots.")

                # remove job from list
                self.futures[scanId].remove(futures)
                removed += 1

        # clean up bad futures
        for status in badstatuslist:
            removed += self.removefutures(status)

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
        into each segment. At most one per scanId.
        Also pushes mock properties to index.
        """

        if random.uniform(0, 1) < self.mockprob:
            mockparams = random.choice(self.mockset)  # pick up to 1 per scanId
            self.inprefs['simulated_transient'] = [mockparams]

        mindexed = (elastic.indexmocks(self.inprefs, scanId)
                    if self.indexresults else 0)
        if mindexed:
            logger.info("Indexed {0} mock transient.".format(mindexed))
        else:
            logger.info("Not indexing mock transient.")

#            if self.tags is None:
#                self.tags = ['mock']
#            elif 'mock' not in self.tags:
#                self.tags = self.tags.append('mock')
#        elif self.tags is not None:
#            if 'mock' in self.tags:
#                self.tags = self.tags.remove('mock')

    def removefutures(self, status, keep=False):
        """ Remove jobs with status of 'cancelled'
        keep argument defines whether to remove or move to futures_removed
        """

        removed = 0
        for scanId in self.futures:

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

                if keep:
                    if scanId not in self.futures_removed:
                        self.futures_removed[scanId] = []
                    self.futures_removed[scanId].append(futures)

        if removed:
            logger.info("Removing {0} bad jobs from scanId {1}".format(removed,
                                                                       scanId))

        return removed

    def handle_finish(self, dataset):
        """ Triggered when obs doc defines end of a script.
        """

        logger.info('End of scheduling block message received.')


def worker_memory_ready(cl, memory_required):
    """ Does any READER worker have enough memory?
    memory_required is the size of the read in bytes
    """

    for vals in itervalues(cl.scheduler_info()['workers']):
        # look for at least one worker with required memory
        if (('READER' in vals['resources']) and
           (vals['memory_limit']-vals['memory'] > memory_required)):
            return True

    logger.info("No worker found with required memory of {0} GB"
                .format(memory_required/1e9))

    return False


def total_memory_ready(cl, memory_limit):
    """ Is total READER memory usage too high?
    memory_limit is total memory used in bytes
    TODO: do we need to add a direct check of dask-worker-space directory?
    """

    if memory_limit is not None:
        total = sum([v['memory']
                    for v in itervalues(cl.scheduler_info()['workers'])
                    if 'READER' in v['resources']])

        if total > memory_limit:
            logger.info("Total memory of {0} GB in use. Exceeds limit of {1} GB."
                        .format(total/1e9, memory_limit/1e9))

        return total < memory_limit
    else:
        return True


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

    # 5) only two antennas
    if len(antnames) <= 2:
        logger.warn("Only {0} antennas in array".format(len(antnames)))
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
                   savebdfdir='/lustre/evla/wcbe/data/no_archive/'):
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
    segment = candcollection.segment
    if not isinstance(segment, int):
        logger.warn("Cannot get unique segment from candcollection ({0})".format(segment))
    st = candcollection.state

    sdmlocs = []
    candranges = gencandranges(candcollection)  # finds time windows to save from segment
    if len(candcollection):
        data = datafuture.result()
        ninttot, nbl, nchantot, npol = data.shape
    else:
        logger.info('No candidate time ranges. Not calling for data.')

    nchan = metadata.nchan_orig//metadata.nspw_orig
    nspw = metadata.nspw_orig
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
                                bdfdir=savebdfdir)
        else:
            logger.warn("No sdm/bdf made for start/end time {0}-{1}"
                        .format(startTime, endTime))

    return sdmlocs


def gencandranges(candcollection):
    """ Given a candcollection, define a list of candidate time ranges.
    """

    segment = candcollection.segment
    st = candcollection.state

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

    # move summary plot too
    summaryplot = '{0}/cands_{1}.png'.format(workdir, scanId)
    summaryplotdest = os.path.join(destination, os.path.basename(summaryplot))
    if os.path.exists(summaryplot):
        shutil.move(summaryplot, summaryplotdest)

    return nplots


def makesummaryplot(workdir, scanId):
    """ Create summary plot for a given scanId and move it
    """

    candsfile = '{0}/cands_{1}.pkl'.format(workdir, scanId)
    candidates.makesummaryplot(candsfile)


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
