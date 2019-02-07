from __future__ import print_function, division, absolute_import#, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import pickle
import os.path
from datetime import date
import random
import distributed
from astropy import time
from time import sleep
import numpy as np
import dask.utils
from evla_mcast.controller import Controller
from rfpipe import state, preferences, metadata, calibration, fileLock
from realfast import pipeline, elastic, mcaf_servers, heuristics, util

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
#_distributed_host = '192.168.201.101'  # for ib0 on cbe-node-01
_distributed_host = '10.80.200.201'  # for ib0 on rfnode021
_default_daskdir = '/lustre/evla/test/realfast/dask-worker-space'


# to parse tuples in yaml
class PrettySafeLoader(yaml.SafeLoader):
    def construct_python_tuple(self, node):
        return tuple(self.construct_sequence(node))


PrettySafeLoader.add_constructor(u'tag:yaml.org,2002:python/tuple',
                                 PrettySafeLoader.construct_python_tuple)


class realfast_controller(Controller):

    def __init__(self, preffile=None, inprefs={}, host=None, **kwargs):
        """ Creates controller object that can act on a scan configuration.
        Inherits a "run" method that starts asynchronous operation that calls
        handle_config. handle_sdm and handle_meta (with datasource "vys" or
        "sim") are also supported.
        host allows specification of 'localhost' for distributed client.

        kwargs can include:
        - tags, a comma-delimited string for cands to index
        - nameincludes, a string required to be in datasetId,
        - vys_timeout, factor over real-time for vys reading to wait,
        - vys_sec_per_spec, time in sec to allow for vys reading (overloaded by vys_timeout)
        - mockprob, chance (range 0-1) that a mock is added to scan,
        - saveproducts, boolean defining generation of mini-sdm,
        - indexresults, boolean defining push (meta)data to search index,
        - archiveproducts, boolean defining archiving mini-sdm,
        - throttle, integer defining slowing pipeline submission relative to realtime,
        - read_overhead, throttle param requires multiple of vismem in a READERs memory,
        - read_totfrac, throttle param requires fraction of total READER memory be available,
        - spill_limit, throttle param limiting maximum size (in GB) of data spill directory,
        - searchintents, a list of intent names to search,
        - indexprefix, a string defining set of indices to save results.
        """

        super(realfast_controller, self).__init__()

        self.inprefs = inprefs  # rfpipe preferences
        if host is None:
            self.client = distributed.Client('{0}:{1}'
                                             .format(_distributed_host,
                                                     '8786'))
        elif host == 'localhost':
            self.client = distributed.Client(n_workers=1,
                                             threads_per_worker=2,
                                             resources={"READER": 1,
                                                        "GPU": 1,
                                                        "MEMORY": 10e9})
        else:
            self.client = distributed.Client('{0}:{1}'
                                             .format(host, '8786'))

        self.lock = dask.utils.SerializableLock()
        self.states = {}
        self.futures = {}
        self.futures_removed = {}
        self.finished = {}
        self.errors = {}

        # define attributes from yaml file
        self.preffile = preffile if preffile is not None else _preffile
        prefs = {}
        if os.path.exists(self.preffile):
            with open(self.preffile, 'r') as fp:
                prefs = yaml.load(fp, Loader=PrettySafeLoader)['realfast']
                logger.info("Parsed realfast preferences from {0}"
                            .format(self.preffile))

                _ = self.client.run(preferences.parsepreffile, self.preffile, asynchronous=True)
        else:
            logger.warn("realfast preffile {0} given, but not found"
                        .format(self.preffile))

        # get arguments from preffile, optional overload from kwargs
        for attr in ['tags', 'nameincludes', 'mockprob', 'vys_timeout',
                     'vys_sec_per_spec', 'indexresults', 'saveproducts',
                     'archiveproducts', 'searchintents', 'throttle',
                     'read_overhead', 'read_totfrac', 'spill_limit',
                     'indexprefix', 'daskdir', 'requirecalibration',
                     'data_logging']:
            if attr == 'indexprefix':
                setattr(self, attr, 'new')
            elif attr == 'throttle':
                setattr(self, attr, 0.8) # submit relative to realtime
            else:
                setattr(self, attr, None)

            if attr in prefs:
                setattr(self, attr, prefs[attr])
            if attr in kwargs:
                setattr(self, attr, kwargs[attr])

        if self.indexprefix is None:
            self.indexprefix = 'new'
        assert self.indexprefix in ['new', 'test', 'aws'], "indexprefix must be None, 'new', 'test' or 'aws'."
        if self.daskdir is None:
            self.daskdir = _default_daskdir

        # TODO: set defaults for these
        assert self.read_overhead and self.read_totfrac and self.spill_limit

    def __repr__(self):
        nseg = len([seg for (scanId, futurelist) in iteritems(self.futures)
                    for seg, data, cc, acc in futurelist])

        return ('realfast controller with {0} jobs'
                .format(nseg))

    @property
    def statuses(self):
        for (scanId, futurelist) in iteritems(self.futures):
            for seg, data, cc, acc in futurelist:
                if len(self.client.who_has()[data.key]):
                    dataloc = self.workernames[self.client.who_has()[data.key][0]]
                    logger.info('{0}, {1}: {2}, {3}, {4}. Data on {5}.'
                                .format(scanId, seg, data.status, cc.status,
                                        acc.status, dataloc))
                else:
                    logger.info('{0}, {1}: {2}, {3}, {4}.'
                                .format(scanId, seg, data.status, cc.status,
                                        acc.status))

    @property
    def exceptions(self):
        return ['{0}, {1}: {2}, {3}'.format(scanId, seg, data.exception(),
                                            cc.exception())
                for (scanId, futurelist) in iteritems(self.futures)
                for seg, data, cc, acc in futurelist
                if data.status == 'error' or cc.status == 'error']

    @property
    def processing(self):
        return dict((self.workernames[k], v)
                    for k, v in iteritems(self.client.processing()) if v)

    @property
    def workernames(self):
        return dict((k, v['id'])
                    for k, v in iteritems(self.client.scheduler_info()['workers']))

    @property
    def reader_memory_available(self):
        return heuristics.reader_memory_available(self.client)

    @property
    def reader_memory_used(self):
        return heuristics.reader_memory_used(self.client)

    @property
    def spilled_memory(self):
        return heuristics.spilled_memory(self.daskdir)

    @property
    def pending(self):
        """ Show number of segments in scanId that are still pending
        """

        return dict([(scanId,
                      len(list(filter(lambda x: x[3].status == 'pending',
                                      futurelist))))
                     for scanId, futurelist in iteritems(self.futures)])

    def restart(self):
        self.client.restart()

    def handle_config(self, config, cfile=_vys_cfile_prod, segments=None):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        Default vys config file uses production parameters.
        segments arg can be used to submit a subset of all segments.

        """

        summarize(config)

        if search_config(config, preffile=self.preffile, inprefs=self.inprefs,
                         nameincludes=self.nameincludes,
                         searchintents=self.searchintents):
            logger.info('Config looks good. Generating rfpipe state...')

            self.set_state(config.scanId, config=config,
                           inmeta={'datasource': 'vys'})

            self.start_pipeline(config.scanId, cfile=cfile, segments=segments)

        else:
            logger.info("Config not suitable for realfast. Skipping.")

        self.cleanup()

    def handle_sdm(self, sdmfile, sdmscan, bdfdir=None, segments=None):
        """ Parallel to handle_config, but allows sdm to be passed in.
        segments arg can be used to submit a subset of all segments.
        """

        # TODO: subscan assumed = 1
        sdmsubscan = 1
        scanId = '{0}.{1}.{2}'.format(os.path.basename(sdmfile.rstrip('/')),
                                      str(sdmscan), str(sdmsubscan))

        self.set_state(scanId, sdmfile=sdmfile, sdmscan=sdmscan, bdfdir=bdfdir,
                       inmeta={'datasource': 'sdm'})

        self.start_pipeline(scanId, segments=segments)

        self.cleanup()

    def handle_meta(self, inmeta, cfile=_vys_cfile_test, segments=None):
        """ Parallel to handle_config, but allows metadata dict to be passed in.
        Gets called explicitly.
        Default vys config file uses test parameters.
        inmeta datasource key ('vys', 'sim', or 'vyssim') is passed to rfpipe.
        segments arg can be used to submit a subset of all segments.
        """

        scanId = '{0}.{1}.{2}'.format(inmeta['datasetId'], str(inmeta['scan']),
                                      str(inmeta['subscan']))

        self.set_state(scanId, inmeta=inmeta)

        self.start_pipeline(scanId, cfile=cfile, segments=segments)

        self.cleanup()

    def set_state(self, scanId, config=None, inmeta=None, sdmfile=None,
                  sdmscan=None, bdfdir=None):
        """ Given metadata source, define state for a scanId.
        Uses metadata to set preferences used in preffile (prefsname).
        Preferences are then overloaded with self.inprefs.
        Will inject mock transient based on mockprob and other parameters.
        """

        prefsname = get_prefsname(inmeta=inmeta, config=config,
                                  sdmfile=sdmfile, sdmscan=sdmscan,
                                  bdfdir=bdfdir)

        inprefs = preferences.parsepreffile(self.preffile, name=prefsname,
                                            inprefs=self.inprefs)

        # alternatively, overload prefs with compiled rules (req Python>= 3.5)
#        inprefs = {**inprefs, **heuristics.band_prefs(inmeta)}

        st = state.State(inmeta=inmeta, config=config, inprefs=inprefs,
                         lock=self.lock, sdmfile=sdmfile, sdmscan=sdmscan,
                         bdfdir=bdfdir)

        logger.info('State set for scanId {0}. Requires {1:.1f} GB read and'
                    ' {2:.1f} GPU-sec to search.'
                    .format(st.metadata.scanId,
                            heuristics.total_memory_read(st),
                            heuristics.total_compute_time(st)))

        self.states[scanId] = st

    def start_pipeline(self, scanId, cfile=None, segments=None):
        """ Start pipeline conditional on cluster state.
        Sets futures and state after submission keyed by scanId.
        segments arg can be used to select or slow segment submission.
        """

        st = self.states[scanId]
        w_memlim = self.read_overhead*st.vismem*1e9
        if segments is None:
            segments = list(range(st.nsegment))

        vys_timeout = self.vys_timeout
        if st.metadata.datasource in ['vys', 'vyssim']:
            if self.vys_timeout is not None:
                logger.info("vys_timeout factor set to fixed value of {0:.1f}x"
                            .format(vys_timeout))
            else:
                assert self.vys_sec_per_spec is not None, "Must define vys_sec_per_spec to estimate vys_timeout"
                nspec = st.readints*st.nbl*st.nspw*st.npol
                vys_timeout = (st.t_segment + self.vys_sec_per_spec*nspec)/st.t_segment
                logger.info("vys_timeout factor scaled by nspec to {0:.1f}x"
                            .format(vys_timeout))

        mockseg = random.choice(segments) if random.uniform(0, 1) < self.mockprob else None
        if mockseg is not None:
            logger.info("Mock set for scanId {0} in segment {1}"
                        .format(scanId, mockseg))

        # vys data means realtime operations must timeout within a scan time
        if st.metadata.datasource == 'vys':
            timeout = 0.9*st.metadata.inttime*st.metadata.nints  # bit shorter than scan
        else:
            timeout = 0
        throttletime = self.throttle*st.metadata.inttime*st.metadata.nints/st.nsegment
        logger.info('Submitting segments for scanId {0} with throttletime {1:.1f}'
                    'read_overhead {2}, read_totfrac {3}, and '
                    'spill_limit {4} with timeout {5}s'
                    .format(scanId, throttletime, self.read_overhead, self.read_totfrac,
                            self.spill_limit, timeout))

        tot_memlim = self.read_totfrac*sum([v['memory_limit']
                                            for v in itervalues(self.client.scheduler_info()['workers'])
                                            if 'READER' in v['resources']])

        # submit segments
        t0 = time.Time.now().unix
        elapsedtime = 0
        nsegment = 0
        for segment in segments:
            segsubtime = time.Time.now().unix

            if st.metadata.datasource == 'vys':
                endtime = time.Time(st.segmenttimes[segment][1], format='mjd').unix
                if endtime < segsubtime:
                    logger.warn("Segment {0} time window has passed ({1} < {2}). Skipping."
                                .format(segment, endtime, segsubtime))
                    continue

            # submit if cluster ready and telcal available
            if (heuristics.reader_memory_ok(self.client, w_memlim) and
                heuristics.readertotal_memory_ok(self.client, tot_memlim) and
                heuristics.spilled_memory_ok(limit=self.spill_limit,
                                             daskdir=self.daskdir) and
                (self.set_telcalfile(scanId)
                 if self.requirecalibration else True)):

                # first time initialize scan
                if scanId not in self.futures:
                    self.futures[scanId] = []
                    self.errors[scanId] = 0
                    self.finished[scanId] = 0
                    if self.indexresults:
                        elastic.indexscan(inmeta=self.states[scanId].metadata,
                                          preferences=self.states[scanId].prefs,
                                          indexprefix=self.indexprefix)
                    else:
                        logger.info("Not indexing scan or prefs.")

                futures = pipeline.pipeline_seg(st, segment, cl=self.client,
                                                cfile=cfile,
                                                vys_timeout=vys_timeout,
                                                mem_read=w_memlim,
                                                mem_search=2*st.vismem*1e9,
                                                mockseg=mockseg)
                self.futures[scanId].append(futures)
                nsegment += 1

                if self.data_logging:
                    segment, data, cc, acc = futures
                    distributed.fire_and_forget(self.client.submit(util.data_logger,
                                                                   st, segment,
                                                                   data,
                                                                   fifo_timeout='0s',
                                                                   priority=-1))
                if self.indexresults:
                    elastic.indexscanstatus(scanId, nsegment=nsegment,
                                            pending=self.pending[scanId],
                                            finished=self.finished[scanId],
                                            errors=self.errors[scanId],
                                            indexprefix=self.indexprefix)

            else:
                if not heuristics.reader_memory_ok(self.client, w_memlim):
                    logger.info("No reader available with required memory {0}"
                                .format(w_memlim))
                elif not heuristics.readertotal_memory_ok(self.client,
                                                          tot_memlim):
                    logger.info("Total reader memory exceeds limit of {0}"
                                .format(tot_memlim))
                elif not heuristics.spilled_memory_ok(limit=self.spill_limit,
                                                      daskdir=self.daskdir):
                    logger.info("Spilled memory exceeds limit of {0}"
                                .format(self.spill_limit))
                elif not (self.set_telcalfile(scanId)
                          if self.requirecalibration else True):
                    logger.info("No telcalfile available for {0}"
                                .format(scanId))

            # periodically check on submissions. always, if memory limited.
            if not (segment % 2) or not (heuristics.reader_memory_ok(self.client, w_memlim) and
                                         heuristics.readertotal_memory_ok(self.client, tot_memlim) and
                                         heuristics.spilled_memory_ok(limit=self.spill_limit,
                                                                      daskdir=self.daskdir)):
                self.cleanup(keep=scanId)  # do not remove keys of ongoing submission

            # check timeout and wait time for next segment
            elapsedtime = time.Time.now().unix - t0
            if elapsedtime > timeout and timeout:
                logger.info("Submission timed out. Submitted {0} segments of "
                            "ScanId {1}".format(nsegment, scanId))
                break
            else:
                dt = time.Time.now().unix - segsubtime
                if dt < throttletime:
                    logger.info("Waiting {0}s to submit next segment."
                                .format(throttletime-dt))
                    sleep(throttletime-dt)

## not needed with "always throttle" as default
#        else:
#            logger.info('Submitting all segments for scanId {0}'
#                        .format(scanId))
#
#            if self.indexresults:
#                elastic.indexscan(inmeta=self.states[scanId].metadata,
#                                  preferences=self.states[scanId].prefs,
#                                  indexprefix=self.indexprefix)
#            else:
#                logger.info("Not indexing scan or prefs.")
#
#            if (self.set_telcalfile(scanId) if self.requirecalibration else True):
#                futures = pipeline.pipeline_scan(st, segments=segments,
#                                                 cl=self.client, cfile=cfile,
#                                                 vys_timeout=vys_timeout,
#                                                 mem_read=w_memlim,
#                                                 mem_search=2*st.vismem*1e9,
#                                                 throttle=self.throttle,
#                                                 mockseg=mockseg)
#
#                if self.data_logging:
#                    for (segment, data, cc, acc) in futures:
#                        distributed.fire_and_forget(self.client.submit(util.data_logger,
#                                                                       st,
#                                                                       segment,
#                                                                       data,
#                                                                       fifo_timeout='0s',
#                                                                       priority=-1))
#
#                if len(futures):
#                    self.futures[scanId] = futures
#                    self.errors[scanId] = 0
#                    self.finished[scanId] = 0
#                    if self.indexresults:
#                        elastic.indexscanstatus(scanId, nsegment=len(futures),
#                                                pending=self.pending[scanId],
#                                                finished=self.finished[scanId],
#                                               errors=self.errors[scanId],
#                                               indexprefix=self.indexprefix)

    def cleanup(self, badstatuslist=['cancelled', 'error', 'lost'], keep=None):
        """ Clean up job list.
        Scans futures, removes finished jobs, and pushes results to relevant indices.
        badstatuslist can include 'cancelled', 'error', 'lost'.
        keep defines a scanId (string) key that should not be removed from dicts.
        """

        removed = 0
        cindexed = 0
        sdms = 0

        scanIds = [scanId for scanId in self.futures]
        if len(scanIds):
            logger.info("Checking on scanIds: {0}"
                        .format(','.join(scanIds)))

        # clean futures and get finished jobs
        removed = self.removefutures(badstatuslist)
        sdm_futs = []
        for scanId in self.futures:

            # check on finished
            finishedlist = [(seg, data, cc, acc)
                            for (scanId0, futurelist) in iteritems(self.futures)
                            for seg, data, cc, acc in futurelist
                            if (acc.status == 'finished') and
                               (scanId0 == scanId)]
            self.finished[scanId] += len(finishedlist)
            if self.indexresults:
                elastic.indexscanstatus(scanId, pending=self.pending[scanId],
                                        finished=self.finished[scanId],
                                        errors=self.errors[scanId],
                                        indexprefix=self.indexprefix)

            # TODO: make robust to lost jobs
            for futures in finishedlist:
                seg, data, cc, acc = futures
                ncands, mocks = acc.result()

                if self.indexresults and mocks:
                    mindexed = elastic.indexmock(scanId, mocks,
                                                 indexprefix=self.indexprefix)
                else:
                    logger.debug("Not indexing mock transient(s).")
                
                # index noises
                noisefile = self.states[scanId].noisefile
                if os.path.exists(noisefile):
                    if self.indexresults:
                        res = elastic.indexnoises(noisefile, scanId,
                                                  indexprefix=self.indexprefix)
                    else:
                        logger.debug("Not indexing noises for scanId {0}."
                                     .format(scanId))
                else:
                    logger.debug('No noisefile found, no noises indexed.')

                # index cands
                if ncands:
                    if self.indexresults:
                        workdir = self.states[scanId].prefs.workdir
                        # TODO: check on error handling for fire_and_forget
                        distributed.fire_and_forget(self.client.submit(util.indexcands_and_plots,
                                                                       cc,
                                                                       scanId,
                                                                       self.tags,
                                                                       self.indexprefix,
                                                                       workdir,
                                                                       priority=5))
                        cindexed += ncands
                    else:
                        logger.info("Not indexing cands found in scanId {0}"
                                    .format(scanId))
                else:
                    logger.debug('No candidates for a segment from scanId {0}'
                                 .format(scanId))

                # optionally save and archive sdm/bdfs for segment
                if self.saveproducts and ncands:
                    # TODO: be sure this does not slow loop too much
                    sdm_futs.append(self.client.submit(createproducts, cc,
                                                       data,
                                                       self.archiveproducts,
                                                       priority=5))
                else:
                    logger.debug("Not making new SDMs or moving candplots.")

                # remove job from list
                self.futures[scanId].remove(futures)
                removed += 1

# TODO: confirm that removing this improves speed
#        # check on cand indexing and sdm jobs
#        for fut in distributed.as_completed(nc_futs):
#            cindexed += fut.result()
#            logger.info("{0} candidates indexed".format(fut.result()))
#
        for fut in distributed.as_completed(sdm_futs):
            sdmnames = fut.result()
            sdms += len(sdmnames)
            logger.info("SDMs created at {0}".format(sdmnames))

        # clean up self.futures
        removeids = [scanId for scanId in self.futures
                     if (len(self.futures[scanId]) == 0) and (scanId != keep)]
        if removeids:
            logstr = ("No jobs left for scanIds: {0}."
                      .format(', '.join(removeids)))
            if keep is not None:
                logstr += (". Cleaning state and futures dicts (keeping {0})"
                           .format(keep))
            else:
                logstr += ". Cleaning state and futures dicts."
            logger.info(logstr)

            for scanId in removeids:
                _ = self.futures.pop(scanId)
                _ = self.states.pop(scanId)
                _ = self.finished.pop(scanId)
                _ = self.errors.pop(scanId)

#        _ = self.client.run(gc.collect)
        if removed or cindexed or sdms:
            logger.info('Removed {0} jobs, indexed {1} cands, made {2} SDMs.'
                        .format(removed, cindexed, sdms))

    def cleanup_loop(self, timeout=None):
        """ Clean up until all jobs gone or timeout elapses.
        """

        if timeout is None:
            timeout_string = "no"
            timeout = -1
        else:
            timeout_string = "{0} s".format(timeout)

        logger.info("Cleaning up all futures with {0} timeout"
                    .format(timeout_string))

        t0 = time.Time.now().unix
        badstatuslist = ['cancelled', 'error', 'lost']
        while len(self.futures):
            elapsedtime = time.Time.now().unix - t0
            if (elapsedtime > timeout) and (timeout >= 0):
                badstatuslist += ['pending']
            self.cleanup(badstatuslist=badstatuslist)
            sleep(10)

    def set_telcalfile(self, scanId):
        """ Find and set telcalfile in state prefs, if not set already.
        Returns True if set, False if not available.
        """

        st = self.states[scanId]

        if st.gainfile is not None:
            return True
        else:
            gainfile = ''
            today = date.today()
            directory = '/home/mchammer/evladata/telcal/{0}'.format(today.year)
            name = '{0}.GN'.format(st.metadata.datasetId)
            for path, dirs, files in os.walk(directory):
                for f in filter(lambda x: name in x, files):
                    gainfile = os.path.join(path, name)

            if os.path.exists(gainfile) and os.path.isfile(gainfile):
                logger.info("Found telcalfile {0} for scanId {1}."
                            .format(gainfile, scanId))
                st.prefs.gainfile = gainfile
                return True
            else:
                logger.warn("No telcalfile {0} found for scanId {1}."
                            .format(gainfile, scanId))
                return False

    def removefutures(self, badstatuslist=['cancelled', 'error', 'lost'],
                      keep=False):
        """ Remove jobs with status in badstatuslist.
        badstatuslist can be a single status as a string.
        keep argument defines whether to remove or move to futures_removed
        """

        if isinstance(badstatuslist, str):
            badstatuslist = [badstatuslist]

        removed = 0
        for scanId in self.futures:
            # create list of futures (a dict per segment) that are cancelled

            removelist = [(seg, data, cc, acc)
                          for (scanId0, futurelist) in iteritems(self.futures)
                          for seg, data, cc, acc in futurelist
                          if ((data.status in badstatuslist) or
                              (cc.status in badstatuslist) or
                              (acc.status in badstatuslist)) and
                             (scanId0 == scanId)]

            self.errors[scanId] += len(removelist)
            for removefuts in removelist:
                (seg, data, cc, acc) = removefuts
                logger.warn("scanId {0} segment {1} bad status: {2}, {3}, {4}"
                            .format(scanId, seg, data.status, cc.status,
                                    acc.status))

            # clean them up
            errworkers = [(fut, self.client.who_has(fut))
                          for futs in removelist
                          for fut in futs[1:] if fut.status == 'error']
            errworkerids = [(fut, self.workernames[worker[0][0]])
                            for fut, worker in errworkers
                            for ww in listvalues(worker) if ww]
            for i, errworkerid in enumerate(errworkerids):
                fut, worker = errworkerid
                logger.warn("Error on workers {0}: {1}"
                            .format(worker, fut.exception()))

            for futures in removelist:
                self.futures[scanId].remove(futures)
                removed += 1

                if keep:
                    if scanId not in self.futures_removed:
                        self.futures_removed[scanId] = []
                    self.futures_removed[scanId].append(futures)

        if removed:
            logger.warn("{0} bad jobs removed from scanId {1}".format(removed,
                                                                      scanId))

        return removed

    def handle_finish(self, dataset):
        """ Triggered when obs doc defines end of a script.
        """

        logger.info('End of scheduling block message received.')


def search_config(config, preffile=None, inprefs={},
                  nameincludes=None, searchintents=None):
    """ Test whether configuration specifies a scan config that realfast should
    search
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

    # 6) only if state validates
    prefsname = get_prefsname(config=config)
    if not heuristics.state_validates(config=config, preffile=preffile,
                                      prefsname=prefsname, inprefs=inprefs):
        logger.warn("State not valid for scanId {0}"
                    .format(config.scanId))
        return False
    # 7) only if some fast sampling is done
    t_fast = 0.5
    if not any([inttime < t_fast for inttime in inttimes]):
        logger.warn("No subband has integration time faster than {0} s"
                    .format(t_fast))
        return False

    return True


def get_prefsname(inmeta=None, config=None, sdmfile=None, sdmscan=None,
                  bdfdir=None):
    """ Given a scan, set the name of the realfast preferences to use
    Allows configuration of pipeline based on scan properties.
    (e.g., galactic/extragal, FRB/pulsar).
    """

    meta = metadata.make_metadata(inmeta=inmeta, config=config,
                                  sdmfile=sdmfile, sdmscan=sdmscan,
                                  bdfdir=bdfdir)

    band = heuristics.reffreq_to_band(meta.spw_reffreq)
    if band is not None:
        # currently only 'L' and 'S' are defined
        # TODO: parse preffile to check available prefsnames
        if band in ['C', 'X', 'Ku', 'K', 'Ka', 'Q']:
            band = 'S'
        prefsname = 'NRAOdefault' + band
    else:
        prefsname = 'default'

    return prefsname


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
        logger.info('\t({0}/{1}) ints at (HW/Final) integration time of ({2:.3f}/{3:.3f}) s'
                    .format(int(round(dt/sb0.hw_time_res)),
                            int(round(dt/sb0.final_time_res)),
                            sb0.hw_time_res, sb0.final_time_res))
    except:
        logger.warn("Failed to fully parse config to print summary."
                    "Proceeding.")


def createproducts(candcollection, data, archiveproducts=False,
                   savebdfdir='/lustre/evla/wcbe/data/realfast/'):
    """ Create SDMs and BDFs for a given candcollection (time segment).
    Takes data future and calls data only if windows found to cut.
    This uses the mcaf_servers module, which calls the sdm builder server.
    Currently BDFs are moved to no_archive lustre area by default.
    """

    if isinstance(candcollection, distributed.Future):
        candcollection = candcollection.result()
    if isinstance(data, distributed.Future):
        data = data.result()

    assert isinstance(data, np.ndarray) and data.dtype == 'complex64'

    if len(candcollection.array) == 0:
        logger.info('No candidates to generate products for.')
        return []

    metadata = candcollection.metadata
    segment = candcollection.segment
    if not isinstance(segment, int):
        logger.warn("Cannot get unique segment from candcollection ({0})"
                    .format(segment))
    st = candcollection.state

    candranges = util.gencandranges(candcollection)  # finds time windows to save from segment
    logger.info('Getting data for candidate time ranges {0}.'
                .format(candranges))

    ninttot, nbl, nchantot, npol = data.shape
    nchan = metadata.nchan_orig//metadata.nspw_orig
    nspw = metadata.nspw_orig

    sdmlocs = []
    for (startTime, endTime) in candranges:
        i = (86400*(startTime-st.segmenttimes[segment][0])/metadata.inttime).astype(int)
        nint = (86400*(endTime-startTime)/metadata.inttime).astype(int)  # TODO: may be off by 1
        logger.info("Cutting {0} ints from int {1} for candidate at {2}"
                    .format(nint, i, startTime))
        data_cut = data[i:i+nint].reshape(nint, nbl, nspw, 1, nchan, npol)

        # TODO: fill in annotation dict as defined in confluence doc on realfast collections
        annotation = {}
        calScanTime = np.unique(calibration.getsols(st)['mjd'])
        if len(calScanTime) > 1:
            logger.warn("Using first of multiple cal times: {0}."
                        .format(calScanTime))
        calScanTime = calScanTime[0]

        sdmloc = mcaf_servers.makesdm(startTime, endTime, metadata.datasetId,
                                      data_cut, calScanTime,
                                      annotation=annotation)
        if sdmloc is not None:
            sdmlocs.append(sdmloc)
            logger.info("Created new SDMs at: {0}".format(sdmloc))
            # TODO: migrate bdfdir to newsdmloc once ingest tool is ready
            mcaf_servers.makebdf(startTime, endTime, metadata, data_cut,
                                 bdfdir=savebdfdir)
            # try archiving it
            try:
                if archiveproducts:
                    runingest(sdmloc)  # TODO: implement this

            except distributed.scheduler.KilledWorker:
                logger.warn("Lost SDM generation due to killed worker.")
        else:
            logger.warn("No sdm/bdf made for {0} with start/end time {1}-{2}"
                        .format(metadata.datasetId, startTime, endTime))

    return sdmlocs


def runingest(sdms):
    """ Call archive tool or move data to trigger archiving of sdms.
    This function will ultimately be triggered by candidate portal.
    """

    NotImplementedError
#    /users/vlapipe/workflows/test/bin/ingest -m -p /home/mctest/evla/mcaf/workspace --file 


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
            prefs = preferences.Preferences(**preferences.parsepreffile(self.preffile,
                                                                        name='default'))
            elastic.indexscan(config=config, preferences=prefs)
