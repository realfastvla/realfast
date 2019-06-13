from __future__ import print_function, division, absolute_import#, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import distributed
from dask import array
from dask.base import tokenize
import numpy as np
from time import sleep
from realfast import util

import logging
logger = logging.getLogger(__name__)
vys_timeout_default = 10


def pipeline_scan(st, segments=None, cl=None, host=None, cfile=None,
                  vys_timeout=vys_timeout_default, mem_read=0., mem_search=0.,
                  throttle=False, mockseg=None):
    """ Given rfpipe state and dask distributed client, run search pipline.
    """

    if cl is None:
        if host is None:
            cl = distributed.Client(n_workers=1, threads_per_worker=16,
                                    resources={"READER": 1, "MEMORY": 16e9},
                                    local_dir="/lustre/evla/test/realfast/scratch")
        else:
            cl = distributed.Client('{0}:{1}'.format(host, '8786'))

    if not isinstance(segments, list):
        segments = list(range(st.nsegment))

    futures = []
    sleeptime = throttle*0.8*st.nints*st.inttime/st.nsegment  # bit shorter than scan
    for segment in segments:
        futures.append(pipeline_seg(st, segment, cl=cl, cfile=cfile,
                                    vys_timeout=vys_timeout, mem_read=mem_read,
                                    mem_search=mem_search, mockseg=mockseg))
        if throttle:
            sleep(sleeptime)

    return futures  # list of tuples of futures (seg, data, cc, acc)


def pipeline_seg(st, segment, cl, cfile=None,
                 vys_timeout=vys_timeout_default, mem_read=0., mem_search=0.,
                 mockseg=None):
    """ Submit pipeline processing of a single segment to scheduler.
    Can use distributed client or compute locally.

    Uses distributed resources parameter to control scheduling of GPUs.
    memreq is required memory in bytes.
    """

    from rfpipe import source

    # set up worker node round robin based on segment
    workers = [w['id'] for w in itervalues(cl.scheduler_info()['workers'])]
#    nodes = list(set([w.split('g')[0] for w in workers if 'g' in w]))
#    workerspernode = list(set([int(w.split('g')[1]) for w in workers if 'g' in w]))
#    allowed = ['{0}g{1}'.format(node, 2*(segment % len(workerspernode)))
#               for node in nodes]  # assumes 2 gpus per worker

    logger.info('Submitted read for observation {0}, scan {1}, segment {2} to {3} workers.'
                .format(st.metadata.datasetId, st.metadata.scan, segment,
                        len(workers)))

    data = cl.submit(source.read_segment, st, segment, timeout=vys_timeout,
                     cfile=cfile, resources={'READER': 1, 'MEMORY': mem_read},
                     retries=3)

    if segment == mockseg:
        st.prefs.simulated_transient = 1
    else:
        st.prefs.simulated_transient = None

    candcollection = cl.submit(prep_and_search, st, segment, data,
#                               resources={'MEMORY': mem_search, 'GPU': 2},
                               resources={'MEMORY': mem_search, 'READER': 1},
                               retries=3)

    acc = cl.submit(analyze_cc, candcollection, retries=3)

    return (segment, data, candcollection, acc)


### helper functions

def prep_and_search(st, segment, data, indexprefix='new', returnsoltime=False):
    """ Reproduces rfpipe.search.prep_and_search but calculates and
    indexes noises.
    """

    from rfpipe import source, search, reproduce, candidates

    ret = source.data_prep(st, segment, data, returnsoltime=returnsoltime)
    if returnsoltime:
        data, soltime = ret
    else:
        data = ret
        soltime = None

    util.calc_and_indexnoises(st, segment, data, indexprefix=indexprefix)

    if st.prefs.fftmode == "cuda":
        candcollection = search.dedisperse_search_cuda(st, segment, data)
    elif st.prefs.fftmode == "fftw":
        candcollection = search.dedisperse_search_fftw(st, segment, data)
    else:
        logger.warning("fftmode {0} not recognized (cuda, fftw allowed)"
                       .format(st.prefs.fftmode))

    candcollection = reproduce.reproduce_candcollection(candcollection, data)

    candcollection.soltime = soltime

    candidates.save_cands(st, candcollection)

    return candcollection


def analyze_cc(cc):
    """ Submittable function to get results of cc in memory
    """
    if isinstance(cc.prefs.simulated_transient, list):
        simulated_transient = cc.prefs.simulated_transient
    else:
        simulated_transient = None
    return len(cc), simulated_transient


def read_segment(st, segment, cfile, vys_timeout):
    """ Wrapper for source.read_segment that secedes from worker
    thread pool
    """

    from rfpipe import source

    logger.info("Reading datasetId {0}, segment {1} locally."
                .format(st.metadata.scanId, segment))

    with distributed.worker_client() as cl_loc:
        fut = cl_loc.submit(source.read_segment, st, segment, cfile,
                            vys_timeout)
        data = fut.result()

    logger.info("Finished reading datasetId {0}, segment {1} locally."
                .format(st.metadata.scanId, segment))

    return data


def mergelists(futlists):
    """ Take list of lists and return single list
    ** TODO: could put logic here to find islands, peaks, etc?
    """

    return [fut for futlist in futlists for fut in futlist]


def lazy_read_segment(st, segment, cfile=None,
                      timeout=vys_timeout_default):
    """ rfpipe read_segment as a dask array.
    equivalent to making delayed version of function and then:
    arr = dask.array.from_delayed(dd, st.datashape, np.complex64).
    """

    from rfpipe import source

    shape = st.datashape
    chunks = ((shape[0],), (shape[1],), (shape[2],), (shape[3],))

    name = 'read_segment-' + tokenize([st, segment])
    dask = {(name, 0, 0, 0, 0): (source.read_segment, st, segment,
                                 cfile, timeout)}

    return array.Array(dask=dask, name=name, chunks=chunks, dtype=np.complex64)
