from __future__ import print_function, division, absolute_import#, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import distributed
from dask import array, delayed
from rfpipe import source, search, util, candidates
from dask.base import tokenize
import numpy as np

import logging
logger = logging.getLogger(__name__)
vys_timeout_default = 10


def pipeline_scan(st, segments=None, cl=None, host=None, cfile=None,
                  vys_timeout=vys_timeout_default, mem_read=0., mem_search=0.):
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
    for segment in segments:
        futures.append(pipeline_seg(st, segment, cl=cl, cfile=cfile,
                                    vys_timeout=vys_timeout, mem_read=mem_read,
                                    mem_search=mem_search))

    return futures  # list of tuples of futures (seg, data, cc, ncands)


def pipeline_seg(st, segment, cl, cfile=None,
                 vys_timeout=vys_timeout_default, mem_read=0., mem_search=0.):
    """ Submit pipeline processing of a single segment to scheduler.
    Can use distributed client or compute locally.

    Uses distributed resources parameter to control scheduling of GPUs.
    memreq is required memory in bytes.
    """

    logger.info('Building dask for observation {0}, scan {1}, segment {2}.'
                .format(st.metadata.datasetId, st.metadata.scan, segment))

    resources = {}

    data = delayed(source.read_segment)(st, segment, timeout=vys_timeout,
                                        cfile=cfile)
    resources[tuple(data.__dask_keys__())] = {'READER': 1, 'MEMORY': mem_read}

    candcollection = delayed(search.prep_and_search)(st, segment, data)
    resources[tuple(candcollection.__dask_keys__())] = {'MEMORY': mem_search}
    if st.fftmode == 'cuda':
        resources[tuple(candcollection.__dask_keys__())]['GPU'] = 1
    ncands = delayed(lambda x: len(x))(candcollection)

    futures_seg = cl.compute((segment, data, candcollection, ncands),
                             resources=resources, priority=st.nsegment-segment)

    return futures_seg


### helper functions (not necessarily in use)

def read_segment(st, segment, cfile, vys_timeout):
    """ Wrapper for source.read_segment that secedes from worker
    thread pool
    """

    logger.info("Reading datasetId {0}, segment {1} locally."
                .format(st.metadata.scanId, segment))

    with distributed.worker_client() as cl_loc:
        fut = cl_loc.submit(source.read_segment, st, segment, cfile,
                            vys_timeout)
        data = fut.result()

    logger.info("Finished reading datasetId {0}, segment {1} locally."
                .format(st.metadata.scanId, segment))

    return data


def prep_and_search(st, segment, data):
    """ Wrapper for search.prep_and_search that secedes from worker
    thread pool
    """

    logger.info("Searching datasetId {0}, segment {1} locally."
                .format(st.metadata.scanId, segment))

    with distributed.worker_client() as cl_loc:
        fut = cl_loc.submit(search.prep_and_search, st, segment, data)
        cc = fut.result()

    logger.info("Finished searching datasetId {0}, segment {1} locally."
                .format(st.metadata.scanId, segment))

    return cc


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

    shape = st.datashape
    chunks = ((shape[0],), (shape[1],), (shape[2],), (shape[3],))

    name = 'read_segment-' + tokenize([st, segment])
    dask = {(name, 0, 0, 0, 0): (source.read_segment, st, segment,
                                 cfile, timeout)}

    return array.Array(dask=dask, name=name, chunks=chunks, dtype=np.complex64)
