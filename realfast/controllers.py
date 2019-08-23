from __future__ import print_function, division, absolute_import#, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import pickle
import os.path
from datetime import date
import gc
import random
import distributed
from astropy import time
from time import sleep
import dask.utils
from evla_mcast.controller import Controller
from realfast import pipeline, elastic, heuristics, util

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
_distributed_host = '10.80.200.201:8786'  # for ib0 on rfnode021
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
        host is ip:port for distributed scheduler server.
        host allows specification of 'localhost' for distributed client.

        kwargs can include:
        - tags, a comma-delimited string for cands to index
        - nameincludes, a comma-delimited list of strings required to be in datasetId,
        - vys_timeout, factor over real-time for vys reading to wait,
        - vys_sec_per_spec, time in sec to allow for vys reading (overloaded by vys_timeout)
        - mockprob, chance (range 0-1) that a mock is added to scan,
        - createproducts, boolean defining generation of mini-sdm,
        - indexresults, boolean defining push (meta)data to search index,
        - classify, run fetch classifier on its own gpu,
        - throttle, integer defining slowing pipeline submission relative to realtime,
        - read_overhead, throttle param requires multiple of vismem in a READERs memory,
        - read_totfrac, throttle param requires fraction of total READER memory be available,
        - searchintents, a list of intent names to search,
        - ignoreintents, a list of intent names to not search,
        - indexprefix, a string defining set of indices to save results.
        """

        super(realfast_controller, self).__init__()

        self.inprefs = inprefs  # rfpipe preferences
        if host is None:
            self.client = distributed.Client(_distributed_host)
        elif host == 'localhost':
            self.client = distributed.Client(n_workers=1,
                                             threads_per_worker=2,
                                             resources={"READER": 1,
#                                                        "GPU": 1,
                                                        "MEMORY": 10e9})
        else:
            self.client = distributed.Client(host)

        self.lock = dask.utils.SerializableLock()
        self.states = {}
        self.futures = {}
        self.futures_removed = {}
        self.finished = {}
        self.errors = {}
        self.known_segments = {}

        # define attributes from yaml file
        self.preffile = preffile if preffile is not None else _preffile
        prefs = {}
        if os.path.exists(self.preffile):
            with open(self.preffile, 'r') as fp:
                prefs = yaml.load(fp, Loader=PrettySafeLoader)['realfast']
                logger.info("Parsed realfast preferences from {0}"
                            .format(self.preffile))

        else:
            logger.warn("realfast preffile {0} given, but not found"
                        .format(self.preffile))

        # initialize worker imports and wisdom (really slow!)
#        _ = self.initialize()

        # get arguments from preffile, optional overload from kwargs
        allattrs = ['tags', 'nameincludes', 'mockprob', 'vys_timeout',
                    'vys_sec_per_spec', 'indexresults', 'createproducts',
                    'searchintents',  'ignoreintents',
                    'throttle', 'classify',
                    'read_overhead', 'read_totfrac',
                    'indexprefix', 'daskdir', 'requirecalibration',
                    'data_logging']

        for attr in allattrs:
            if attr == 'indexprefix':
                setattr(self, attr, 'new')
            elif attr == 'throttle':
                setattr(self, attr, 0.8)  # submit relative to realtime
            else:
                setattr(self, attr, None)

            if attr in prefs:
                setattr(self, attr, prefs[attr])
            if attr in kwargs:
                setattr(self, attr, kwargs[attr])

        if self.indexprefix is None:
            self.indexprefix = 'new'
        assert self.indexprefix in ['new', 'test', 'chime', 'aws'], "indexprefix must be None, 'new', 'test', 'chime', or 'aws'."
        if self.daskdir is None:
            self.daskdir = _default_daskdir

        # TODO: set defaults for these
        assert self.read_overhead and self.read_totfrac

        logger.info("Initialized controller with attributes {0} and inprefs {1}"
                    .format([(attr, getattr(self, attr)) for attr in allattrs], self.inprefs))

    def __repr__(self):
        return ('realfast controller with {0} jobs'
                .format(self.nsegment))

    @property
    def nsegment(self):
        """ Total number of segments submitted but not yet cleaned up
        """

        return len([seg for (scanId, futurelist) in iteritems(self.futures)
                    for seg, data, cc, acc in futurelist])

    @property
    def statuses(self):
        for (scanId, futurelist) in iteritems(self.futures):
            for seg, data, cc, acc in futurelist:
                if len(self.client.who_has()[data.key]):
                    try:
                        dataloc = self.workernames[self.client.who_has()[data.key][0]]
                    except KeyError:
                        dataloc = '[key lost]'
                    logger.info('{0}, {1}: {2}, {3}, {4}. Data on {5}.'
                                .format(scanId, seg, data.status, cc.status,
                                        acc.status, dataloc))
                else:
                    logger.info('{0}, {1}: {2}, {3}, {4}.'
                                .format(scanId, seg, data.status, cc.status,
                                        acc.status))

    @property
    def ncands(self):
        for (scanId, futurelist) in iteritems(self.futures):
            for seg, data, cc, acc in futurelist:
                if acc.status == 'finished':
                    ncands, mocks = acc.result()
                    logger.info('{0}, {1}: {2} candidates'
                                .format(scanId, seg, ncands))
                else:
                    logger.info('{0}, {1}: search not complete'
                                .format(scanId, seg))

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
    def fetchworkers(self):
        workers = [w for w in itervalues(self.workernames) if 'fetch' in w]
        if self.classify:
            if len(workers):
                return workers
            else:
                return list(itervalues(self.workernames))
        else:
            return []

    @property
    def reader_memory_available(self):
        return heuristics.reader_memory_available(self.client)

    @property
    def reader_memory_used(self):
        return heuristics.reader_memory_used(self.client)

    @property
    def spilled_memory(self):
        """ Check dask disk cache. Super slow!
        """
        return heuristics.spilled_memory(self.daskdir)

    @property
    def pending(self):
        """ Show number of segments in scanId that are still pending
        """

        return dict([(scanId,
                      len(list(filter(lambda x: x[3].status == 'pending',
                                      futurelist))))
                     for scanId, futurelist in iteritems(self.futures)])

    def initialize(self):
        """ Check versions and run imports on workers to set them up for work.
        """

        from time import sleep

        while len(self.workernames.values()) == 0:
            logger.info("Waiting for workers to start...")
            sleep(5)

        logger.info("Initializing workers: {0}".format(list(self.workernames.values())))
#        logger.info("This should complete in about one minute, but sometimes fails. Use ctrl-c if it takes too long.")
#        _ = self.client.run(util.initialize_worker)
        jobs = [self.client.submit(util.initialize_worker, workers=w, pure=False) for w in list(self.workernames.values())]
        try:
            while True:
                if all([job.status=='finished' for job in jobs]):
                    break
                else:
                    sleep(1)
        except KeyboardInterrupt:
            logger.warn("Exiting worker initialization. Some workers may still take time to start up.")

        try:
            versions = self.client.get_versions(check=True)
        except ValueError:
            logger.warn("Scheduler/worker version mismatch")

    def restart(self):
        self.client.restart()
        sleep(5)
        self.states = {}
        self.futures = {}
        self.futures_removed = {}
        self.finished = {}
        self.errors = {}
        self.known_segments = {}

    def handle_config(self, config, cfile=_vys_cfile_prod, segments=None):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        Default vys config file uses production parameters.
        segments arg can be used to submit a subset of all segments.

        """

        summarize(config)

        if search_config(config, preffile=self.preffile, inprefs=self.inprefs,
                         nameincludes=self.nameincludes,
                         searchintents=self.searchintents,
                         ignoreintents=self.ignoreintents):

            # starting config of an OTF row will trigger subscan logic
            if config.otf:
                logger.info("Good OTF config: calling handle_subscan")
                self.handle_subscan(config, cfile=cfile)
            else:
                logger.info("Good Non-OTF config: setting state and starting pipeline")
                # for standard pointed mode, just set state and start pipeline
                self.set_state(config.scanId, config=config,
                               inmeta={'datasource': 'vys'})

                self.start_pipeline(config.scanId, cfile=cfile,
                                    segments=segments)

        else:
            logger.info("Config not suitable for realfast. Skipping.")

        self.cleanup()

    def handle_subscan(self, config, cfile=_vys_cfile_prod):
        """ Triggered when subscan info is updated (e.g., OTF mode).
        OTF requires more setup and management.
        """

        # catch subscans with wrong name
        if self.nameincludes is not None:
            if any([name in config.datasetId for name in self.nameincludes.split(',')]):
                logger.warn("datasetId {0} not in nameincludes {1}"
                            .format(config.datasetId, self.nameincludes))
                return

        # set up OTF info
        # search pipeline needs [(startmjd, stopmjd, l1, m1), ...]
        phasecenters = []
        endtime_mjd_ = 0
        for ss in config.subscans:
            if ss.stopTime is not None:
                if ss.stopTime > endtime_mjd_:
                    endtime_mjd_ = ss.stopTime
                phasecenters.append((ss.startTime, ss.stopTime,
                                     ss.ra_deg, ss.dec_deg))
        t0 = min([startTime for (startTime, _, _, _) in phasecenters])
        t1 = max([stopTime for (_, stopTime, _, _) in phasecenters])
        logger.info("Calculated {0} phasecenters from {1} to {2}"
                    .format(len(phasecenters), t0, t1))

        # pass in first subscan and overload end time
        config0 = config.subscans[0]  # all tracked by first config of scan
        if config0.is_complete:
            if config0.scanId in self.known_segments:
                logger.info("Already submitted {0} segments for scanId {1}. "
                            "Fast state calculation."
                            .format(len(self.known_segments[config0.scanId]),
                                    config0.scanId))
                self.set_state(config0.scanId, config=config0, validate=False,
                               showsummary=False,
                               inmeta={'datasource': 'vys',
                                       'endtime_mjd_': endtime_mjd_,
                                       'phasecenters': phasecenters})
            else:
                logger.info("No submitted segments for scanId {0}. Submitting "
                            "Slow state calculation.".format(config0.scanId))
                self.set_state(config0.scanId, config=config0,
                               inmeta={'datasource': 'vys',
                                       'endtime_mjd_': endtime_mjd_,
                                       'phasecenters': phasecenters})

            allsegments = list(range(self.states[config0.scanId].nsegment))
            if config0.scanId not in self.known_segments:
                logger.debug("Initializing known_segments with scanId {0}"
                             .format(config0.scanId))
                self.known_segments[config0.scanId] = []

            # get new segments
            segments = [seg for seg in allsegments
                        if seg not in self.known_segments[config0.scanId]]

            # TODO: this may not actually submit if telcal not ready
            # should not update known_segments automatically?
            if len(segments):
                logger.info("Starting pipeline for {0} with segments {1}"
                            .format(config0.scanId, segments))
                self.start_pipeline(config0.scanId, cfile=cfile,
                                    segments=segments)
                # now all (currently known segments) have been started
                logger.info("Updating known_segments for {0} to {1}"
                            .format(config0.scanId, allsegments))
                self.known_segments[config0.scanId] = allsegments
            else:
                logger.info("No new segments to submit for {0}"
                            .format(config0.scanId))
        else:
            logger.info("First subscan config is not complete. Continuing.")

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
                  sdmscan=None, bdfdir=None, validate=True, showsummary=True):
        """ Given metadata source, define state for a scanId.
        Uses metadata to set preferences used in preffile (prefsname).
        Preferences are then overloaded with self.inprefs.
        Will inject mock transient based on mockprob and other parameters.
        """

        from rfpipe import preferences, state

        prefsname = get_prefsname(inmeta=inmeta, config=config,
                                  sdmfile=sdmfile, sdmscan=sdmscan,
                                  bdfdir=bdfdir)

        inprefs = preferences.parsepreffile(self.preffile, name=prefsname,
                                            inprefs=self.inprefs)

        # alternatively, overload prefs with compiled rules (req Python>= 3.5)
#        inprefs = {**inprefs, **heuristics.band_prefs(inmeta)}

        st = state.State(inmeta=inmeta, config=config, inprefs=inprefs,
                         lock=self.lock, sdmfile=sdmfile, sdmscan=sdmscan,
                         bdfdir=bdfdir, validate=validate, showsummary=showsummary)

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
                logger.debug("vys_timeout factor set to fixed value of {0:.1f}x"
                             .format(vys_timeout))
            else:
                assert self.vys_sec_per_spec is not None, "Must define vys_sec_per_spec to estimate vys_timeout"
                nspec = st.readints*st.nbl*st.nspw*st.npol
                vys_timeout = (st.t_segment + self.vys_sec_per_spec*nspec)/st.t_segment
                logger.debug("vys_timeout factor scaled by nspec to {0:.1f}x"
                             .format(vys_timeout))

        mockseg = random.choice(segments) if random.uniform(0, 1) < self.mockprob else None
        if mockseg is not None:
            logger.info("Mock set for scanId {0} in segment {1}"
                        .format(scanId, mockseg))

        # vys data means realtime operations must timeout within a scan time
        if st.metadata.datasource == 'vys':
            timeout = st.metadata.inttime*st.metadata.nints  # bit shorter than scan
        else:
            timeout = 0
        throttletime = self.throttle*st.metadata.inttime*st.metadata.nints/st.nsegment
        logger.info('Submitting {0} segments for scanId {1}'.format(len(segments), scanId))
        logger.debug('Read_overhead {0}, read_totfrac {1}, and '
                     'with timeout {2}s'
                     .format(self.read_overhead, self.read_totfrac, timeout))

        tot_memlim = self.read_totfrac*sum([v['resources']['MEMORY']
                                            for v in itervalues(self.client.scheduler_info()['workers'])
                                            if 'READER' in v['resources']])

        # submit segments
        nsubmitted = 0  # count number submitted from list segments
        justran = 0  # track if function just ran
        segments = iter(segments)
        segment = next(segments)
        telcalset = self.set_telcalfile(scanId)
        t0 = time.Time.now().unix
        while True:
            segsubtime = time.Time.now().unix
            elapsedtime = segsubtime - t0
            if (elapsedtime > timeout) and timeout:
                logger.info("Submission timed out. Submitted {0}/{1} segments "
                            "in ScanId {2}".format(nsubmitted, st.nsegment,
                                                   scanId))
                break

            starttime, endtime = time.Time(st.segmenttimes[segment],
                                           format='mjd').unix
            if st.metadata.datasource in ['vys', 'sim']:
                # TODO: define buffer delay better
                if segsubtime > starttime+2:
                    logger.warning("Segment {0} time window has passed ({1} > {2}). Skipping."
                                   .format(segment, segsubtime, starttime+2))
                    try:
                        segment = next(segments)
                        continue
                    except StopIteration:
                        logger.debug("No more segments for scanId {0}"
                                     .format(scanId))
                        break
                elif segsubtime < starttime-10:
                    logger.info("Waiting {0:.1f}s to submit segment."
                                .format((starttime-10)-segsubtime))
                    sleep((starttime-10)-segsubtime)
            elif st.metadata.datasource == 'sdm':
                sleep(throttletime)

            # try setting telcal
            if not telcalset:
                telcalset = self.set_telcalfile(scanId)
                if telcalset:
                    logger.info("Set calibration for scanId {0}".format(scanId))

            # submit if cluster ready and telcal available
            if (heuristics.reader_memory_ok(self.client, w_memlim) and
                heuristics.readertotal_memory_ok(self.client, tot_memlim) and
                (telcalset if self.requirecalibration else True)):

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
                nsubmitted += 1

                segment, data, cc, acc = futures

                if self.data_logging:
                    distributed.fire_and_forget(self.client.submit(util.data_logger,
                                                                   st, segment,
                                                                   data,
                                                                   retries=3))

                if self.indexresults:
                    distributed.fire_and_forget(self.client.submit(elastic.indexscanstatus,
                                                                   scanId,
                                                                   indexprefix=self.indexprefix,
                                                                   pending=self.pending[scanId],
                                                                   finished=self.finished[scanId],
                                                                   errors=self.errors[scanId],
                                                                   nsegment=st.nsegment,
                                                                   retries=3))

                try:
                    segment = next(segments)
                except StopIteration:
                    logger.info("No more segments for scanId {0}".format(scanId))
                    break

            else:
                if not heuristics.reader_memory_ok(self.client, w_memlim):
                    if not (int(segsubtime-t0) % 20) and not justran:  # every 20 sec
                        logger.info("System not ready. No reader available with required memory {0}"
                                    .format(w_memlim))
                        self.client.run(gc.collect)
                        self.cleanup(keep=scanId)  # do not remove keys of ongoing submission
                        justran = 1
                    elif (int(segsubtime-t0) % 5):
                        justran = 0
                elif not heuristics.readertotal_memory_ok(self.client,
                                                          tot_memlim):
                    if not (int(segsubtime-t0) % 20) and not justran:  # every 20 sec
                        logger.info("System not ready. Total reader memory exceeds limit of {0}"
                                    .format(tot_memlim))
                        self.client.run(gc.collect)
                        self.cleanup(keep=scanId)  # do not remove keys of ongoing submission
                        justran = 1
                    elif (int(segsubtime-t0) % 5):
                        justran = 0
                elif not (telcalset if self.requirecalibration else True):
                    if not (int(segsubtime-t0) % 5) and not justran:  # every 5 sec
                        logger.info("System not ready. No telcalfile available for {0}"
                                    .format(scanId))
                        self.cleanup(keep=scanId)  # do not remove keys of ongoing submission
                        justran = 1
                    elif (int(segsubtime-t0) % 5):
                        justran = 0

            # periodically check on submissions
            if not (int(segsubtime-t0) % 5) and not justran:
                self.cleanup(keep=scanId)  # do not remove keys of ongoing submission
                justran = 1
            elif (int(segsubtime-t0) % 5):
                justran = 0

    def cleanup(self, badstatuslist=['cancelled', 'error', 'lost'], keep=None):
        """ Clean up job list.
        Scans futures, removes finished jobs, and pushes results to relevant indices.
        badstatuslist can include 'cancelled', 'error', 'lost'.
        keep defines a scanId (string) key that should not be removed from dicts.
        """

        removed = 0

        scanIds = [scanId for scanId in self.futures]
        if len(scanIds):
            logger.info("Checking on {0} segments in {1} scanIds: {2}"
                        .format(self.nsegment, len(scanIds),
                                ','.join(scanIds)))

        # run memory logging
        memory_summary = ','.join(['({0}, {1})'.format(v['id'], v['metrics']['memory']/1e9) for k, v in iteritems(self.client.scheduler_info()['workers']) if v['metrics']['memory']/1e9 > 10])
        if memory_summary:
            workers_highmem = [k for k, v in iteritems(self.client.scheduler_info()['workers']) if v['metrics']['memory']/1e9 > 10]
            self.client.run(logging_statement, memory_summary, workers=workers_highmem)
            logger.info("High memory usage on cluster: {0}".format(memory_summary))

        # clean futures and get finished jobs
        removed = self.removefutures(badstatuslist)
        for scanId in self.futures:

            # check on finished
            finishedlist = [(seg, data, cc, acc)
                            for (scanId0, futurelist) in iteritems(self.futures)
                            for seg, data, cc, acc in futurelist
                            if (acc.status == 'finished') and
                               (scanId0 == scanId)]
            self.finished[scanId] += len(finishedlist)
            if self.indexresults:
                distributed.fire_and_forget(self.client.submit(elastic.indexscanstatus,
                                            scanId,
                                            indexprefix=self.indexprefix,
                                            pending=self.pending[scanId],
                                            finished=self.finished[scanId],
                                            errors=self.errors[scanId],
                                            retries=3))

            for futures in finishedlist:
                seg, data, cc, acc = futures

                if self.indexresults:
                    # index mocks from special workers
                    distributed.fire_and_forget(self.client.submit(elastic.indexmock,
                                                                   scanId,
                                                                   acc=acc,
                                                                   indexprefix=self.indexprefix,
#                                                                   workers=self.fetchworkers,
                                                                   retries=3))

                    # index cands and copy data from special workers
                    workdir = self.states[scanId].prefs.workdir
                    distributed.fire_and_forget(self.client.submit(util.indexcands_and_plots,
                                                                   cc,
                                                                   scanId,
                                                                   self.tags,
                                                                   self.indexprefix,
                                                                   workdir,
#                                                                   workers=self.fetchworkers,
                                                                   retries=3))
                    if self.classify:
                        # classify cands on special workers
                        distributed.fire_and_forget(self.client.submit(util.classify_candidates,
                                                                       cc, self.indexprefix,
                                                                       retries=3,
                                                                       workers=self.fetchworkers,
                                                                       resources={'GPU': 1}))

                if self.createproducts:
                    # optionally save and archive sdm/bdfs for segment
                    indexprefix = self.indexprefix if self.indexresults else None
                    distributed.fire_and_forget(self.client.submit(util.createproducts,
                                                                   cc, data,
                                                                   indexprefix=indexprefix,
                                                                   retries=3))

                # remove job from list
                # TODO: need way to report number of cands and sdms before removal without slowing cleanup
                self.futures[scanId].remove(futures)
                removed += 1

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
                try:
                    _ = self.known_segments.pop(scanId)
                except KeyError:
                    pass

        # hack to clean up residual jobs in bokeh
        if not len(self.processing) and len(self.client.who_has()):
            logger.info("Retrying {0} scheduler jobs without futures."
                        .format(len(self.client.who_has())))
            self.cleanup_retry()
            sleep(5)
            self.client.run(gc.collect)

        if removed:
            logger.info('Removed {0} jobs'.format(removed))

    def cleanup_retry(self):
        """ Get futures from client who_has and retry them.
        This will clean up futures showing in scheduler bokeh app.
        """

        futs = []
        for k in self.client.who_has():
            logger.info("Retrying {0}".format(k))
            fut = distributed.Future(k)
            fut.retry()
            futs.append(fut)

        for fut in distributed.as_completed(futs):
            logger.info("Future {0} completed. Releasing it.".format(fut.key))
            fut.release()

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

        from rfpipe.calibration import getsols

        st = self.states[scanId]

        # try to find file
        if st.gainfile is None:
            gainfile = ''
            today = date.today()
            directory = '/home/mchammer/evladata/telcal/{0}/{1:02}'.format(today.year, today.month)
            name = '{0}.GN'.format(st.metadata.datasetId)
            gainfile = os.path.join(directory, name)
            if os.path.exists(gainfile) and os.path.isfile(gainfile):
                logger.debug("Found telcalfile {0} for scanId {1}."
                             .format(gainfile, scanId))
                st.prefs.gainfile = gainfile

        # parse sols to test whether good ones exist
        sols = getsols(st)
        if len(sols) and not all(sols['flagged']):
            return True
        else:
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
                dataloc = self.workernames[self.client.who_has()[data.key][0]]
                logger.warn("scanId {0} segment {1} bad status: {2}, {3}, {4}. Data on {5}"
                            .format(scanId, seg, data.status, cc.status,
                                    acc.status, dataloc))

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


def logging_statement(statement):
    """ Log a statement on a worker
    """

    logger.info("{0}".format(statement))


def search_config(config, preffile=None, inprefs={},
                  nameincludes=None, searchintents=None, ignoreintents=None):
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
    # 1) if nameincludes set, reject if datasetId does not have it
    if nameincludes is not None:
        if any([name in config.datasetId for name in nameincludes.split(',')]):
            logger.warn("datasetId {0} not in nameincludes {1}"
                        .format(config.datasetId, nameincludes))
            return False

    # 2) only if some fast sampling is done (faster than VLASS final inttime)
    t_fast = 0.4
    if not any([inttime < t_fast for inttime in inttimes]):
        logger.warn("No subband has integration time faster than {0} s"
                    .format(t_fast))
        return False

    # 3) only search if in searchintents
    if searchintents is not None:
        if not any([searchintent in intent for searchintent in searchintents]):
            logger.warn("intent {0} not in searchintents list {1}"
                        .format(intent, searchintents))
            return False

    # 4) do not search if in ignoreintents
    if ignoreintents is not None:
        if any([ignoreintent in intent for ignoreintent in ignoreintents]):
            logger.warn("intent {0} not in ignoreintents list {1}"
                        .format(intent, ignoreintents))
            return False

    # 5) chansize changes between subbands
    if not all([chansizes[0] == chansize for chansize in chansizes]):
        logger.warn("Channel size changes between subbands: {0}"
                    .format(chansizes))
        return False

    # 6) start and stop time is after current time
    now = time.Time.now().unix
    startTime = time.Time(config.startTime, format='mjd').unix
    stopTime = time.Time(config.stopTime, format='mjd').unix
    if (startTime < now) and (stopTime < now):
        logger.warn("Scan startTime and stopTime are in the past ({0}, {1} < {2})"
                    .format(startTime, stopTime, now))
        return False

    # 7) only two antennas
    if len(antnames) <= 2:
        logger.warn("Only {0} antennas in array".format(len(antnames)))
        return False

    # 8) only if state validates
    prefsname = get_prefsname(config=config)
    if not heuristics.state_validates(config=config, preffile=preffile,
                                      prefsname=prefsname, inprefs=inprefs):
        logger.warn("State not valid for scanId {0}"
                    .format(config.scanId))
        return False

    return True


def get_prefsname(inmeta=None, config=None, sdmfile=None, sdmscan=None,
                  bdfdir=None):
    """ Given a scan, set the name of the realfast preferences to use
    Allows configuration of pipeline based on scan properties.
    (e.g., galactic/extragal, FRB/pulsar).
    """

    from rfpipe import metadata

    meta = metadata.make_metadata(inmeta=inmeta, config=config,
                                  sdmfile=sdmfile, sdmscan=sdmscan,
                                  bdfdir=bdfdir)

    band = heuristics.reffreq_to_band(meta.spw_reffreq)
    if band is not None:
        # bands higher than X go to X
        if band in ['Ku', 'K', 'Ka', 'Q']:
            band = 'X'
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


class config_controller(Controller):

    def __init__(self, preffile=None, inprefs={}, pklfile=None, **kwargs):
        """ Creates controller object that saves scan configs.
        If pklfile is defined, it will save pickle there.
        If preffile is defined, it will attach a preferences to indexed scan.
        Inherits a "run" method that starts asynchronous operation.
        """

        super(config_controller, self).__init__()
        self.pklfile = pklfile
        self.inprefs = inprefs

        # define attributes from yaml file
        self.preffile = preffile if preffile is not None else _preffile
        prefs = {}
        if os.path.exists(self.preffile):
            with open(self.preffile, 'r') as fp:
                prefs = yaml.load(fp, Loader=PrettySafeLoader)['realfast']
                logger.info("Parsed realfast preferences from {0}"
                            .format(self.preffile))

        else:
            logger.warn("realfast preffile {0} given, but not found"
                        .format(self.preffile))

        # get arguments from preffile, optional overload from kwargs
        for attr in ['tags', 'nameincludes', 'mockprob', 'vys_timeout',
                     'vys_sec_per_spec', 'indexresults', 'createproducts',
                     'searchintents',  'ignoreintents',
                     'throttle',
                     'read_overhead', 'read_totfrac',
                     'indexprefix', 'daskdir', 'requirecalibration',
                     'data_logging']:
            if attr == 'indexprefix':
                setattr(self, attr, 'new')
            elif attr == 'throttle':
                setattr(self, attr, 0.8)  # submit relative to realtime
            else:
                setattr(self, attr, None)

            if attr in prefs:
                setattr(self, attr, prefs[attr])
            if attr in kwargs:
                setattr(self, attr, kwargs[attr])

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        from rfpipe import util

        logger.info('Received complete configuration for {0}, '
                    'scan {1}, subscan {2}, source {3}, intent {4}'
                    .format(config.scanId, config.scanNo, config.subscanNo,
                            config.source, config.scan_intent))

        if self.pklfile:
            with open(self.pklfile, 'ab') as pkl:
                pickle.dump(config, pkl)

        searchable = search_config(config, preffile=self.preffile, inprefs=self.inprefs,
                                   nameincludes=self.nameincludes,
                                   searchintents=self.searchintents,
                                   ignoreintents=self.ignoreintents)
        if searchable:
            message = 'New scan {0}: searchable'.format(config.datasetId)
        else:
            message = 'New scan {0}: not searchable'.format(config.datasetId)

        util.update_slack('#alerts', message)
