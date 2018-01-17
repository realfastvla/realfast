from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import distributed
from dask import delayed
from rfpipe import source, search, util, candidates

import logging
logger = logging.getLogger(__name__)
vys_timeout_default = 10


def pipeline_scan(st, segments=None, host=None, cl=None, cfile=None,
                  vys_timeout=vys_timeout_default):
    """ Given rfpipe state and dask distributed client, run search pipline """

    futures = []
    if not isinstance(segments, list):
        segments = range(st.nsegment)

    # TODO: add wait here to reduce early submission of segments?
    for segment in segments:
        futures.append(pipeline_seg(st, segment, host=host, cl=cl, cfile=cfile,
                                    vys_timeout=vys_timeout))

    return futures  # list of dicts


def pipeline_seg(st, segment, host=None, cl=None, cfile=None,
                 vys_timeout=vys_timeout_default):
    """ Submit pipeline processing of a single segment to scheduler.
    Can use distributed client or compute locally.

    Uses distributed resources parameter to control scheduling of GPUs.
    Pipeline produces jobs per DM/dt.
    Returns a dict with values as futures of certain jobs (data, collection).
    """

    logger.info('Building dask for observation {0}, scan {1}, segment {2}.'
                .format(st.metadata.datasetId, st.metadata.scan, segment))

    if cl is None:
        if host is None:
            cl = distributed.Client(n_workers=1, threads_per_worker=16,
                                    resources={"READER": 1, "MEMORY": 24,
                                               "CORES": 16},
                                    local_dir="/lustre/evla/test/realfast/scratch")
        else:
            cl = distributed.Client('{0}:{1}'.format(host, '8786'))

    futures = {}

    data = cl.submit(source.read_segment, st, segment, timeout=vys_timeout,
                     cfile=cfile, pure=False,
                     resources={'READER': 1, 'MEMORY': 8*st.vismem})
    futures['data'] = data

    data_prep = cl.submit(source.data_prep, st, segment, data,
                          resources={'CORES': st.prefs.nthread})

    saved = []
    if st.fftmode == "fftw":
        searchresources = {'MEMORY': 2*st.immem+2*st.vismem,
                           'CORES': st.prefs.nthread}
        imgranges = [[(min(st.get_search_ints(segment, dmind, dtind)),
                     max(st.get_search_ints(segment, dmind, dtind)))
                      for dtind in range(len(st.dtarr))]
                     for dmind in range(len(st.dmarr))]
        wisdom = cl.submit(search.set_wisdom, st.npixx, st.npixy,
                           resources={'CORES': 1})

        for dmind in range(len(st.dmarr)):
            delay = cl.submit(util.calc_delay, st.freq, st.freq.max(),
                              st.dmarr[dmind], st.inttime,
                              resources={'CORES': 1})
            for dtind in range(len(st.dtarr)):
                data_corr = cl.submit(search.dedisperseresample, data_prep,
                                      delay, st.dtarr[dtind],
                                      parallel=st.prefs.nthread > 1,
                                      resources={'MEMORY': 2*st.vismem,
                                                 'CORES': st.prefs.nthread})

                im0, im1 = imgranges[dmind][dtind]
                integrationlist = [list(range(im0, im1)[i:i+st.chunksize])
                                   for i in range(0, im1-im0, st.chunksize)]
                for integrations in integrationlist:
                    saved.append(cl.submit(search.search_thresh_fftw, st,
                                           segment, data_corr, dmind, dtind,
                                           integrations=integrations,
                                           wisdom=wisdom,
                                           resources=searchresources))

    elif st.fftmode == "cuda":
        for dmind in range(len(st.dmarr)):
            saved.append(cl.submit(search.dedisperse_image_cuda, st, segment,
                                   data_prep, dmind,
                                   resources={'GPU': 1,
                                              'CORES': st.prefs.nthread}))

    canddatalist = cl.submit(mergelists, saved,
                             resources={'CORES': 1})
    candcollection = cl.submit(candidates.calc_features, canddatalist,
                               resources={'CORES': 1})
    futures['candcollection'] = candcollection

    return futures


def pipeline_seg_delayed(st, segment, host=None, cl=None, cfile=None,
                         vys_timeout=vys_timeout_default):
    """ As above, but uses dask delayed to compose graph before computing.
    """

    logger.info('Building dask for observation {0}, scan {1}, segment {2}.'
                .format(st.metadata.datasetId, st.metadata.scan, segment))

    if cl is None:
        if host is None:
            cl = distributed.Client(n_workers=1, threads_per_worker=16,
                                    local_dir="/lustre/evla/test/realfast/scratch")
        else:
            cl = distributed.Client('{0}:{1}'.format(host, '8786'))

    imgranges = [[(min(st.get_search_ints(segment, dmind, dtind)),
                  max(st.get_search_ints(segment, dmind, dtind)))
                  for dtind in range(len(st.dtarr))]
                 for dmind in range(len(st.dmarr))]

    futures = {}

    # plan, if using fftw
    wisdom = delayed(search.set_wisdom, pure=True)(st.npixx, st.npixy)

    # will retry to get around thread collision during read (?)
    data = delayed(source.read_segment, pure=True)(st, segment,
                                                   timeout=vys_timeout,
                                                   cfile=cfile)

    data_prep = delayed(source.data_prep, pure=True)(st, data)

    futures['data'] = cl.compute(data_prep)

    saved = []
    for dmind in range(len(st.dmarr)):
        delay = delayed(util.calc_delay, pure=True)(st.freq, st.freq.max(),
                                                    st.dmarr[dmind], st.inttime)

        for dtind in range(len(st.dtarr)):
            data_corr = delayed(search.dedisperseresample, pure=True)(data_prep, delay,
                                                                      st.dtarr[dtind],
                                                                      parallel = st.prefs.nthread > 1)

            im0, im1 = imgranges[dmind][dtind]
            integrationlist = [list(range(im0, im1)[i:i+st.chunksize])
                               for i in range(0, im1-im0, st.chunksize)]
            for integrations in integrationlist:
                saved.append(delayed(search.search_thresh,
                                     pure=True)(st, segment, data_corr, dmind,
                                                dtind,
                                                integrations=integrations,
                                                wisdom=wisdom))

    canddatalist = delayed(mergelists, pure=True)(saved)
    candcollection = delayed(candidates.calc_features, pure=True)(canddatalist)

    futures['candcollection'] = cl.compute(candcollection)

    return futures


def mergelists(futlists):
    """ Take list of lists and return single list
    ** TODO: could put logic here to find islands, peaks, etc?
    """

    return [fut for futlist in futlists for fut in futlist]
