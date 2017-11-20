from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import distributed
from rfpipe import source, search, util, candidates

import logging
logger = logging.getLogger(__name__)
vys_timeout_default = 10


def pipeline_scan(st, segments=None, host='cbe-node-01', cfile=None,
                  vys_timeout=vys_timeout_default):
    """ Given rfpipe state and dask distributed client, run search pipline """

    cl = distributed.Client('{0}:{1}'.format(host, '8786'))

    futures = []
    if not isinstance(segments, list):
        segments = range(st.nsegment)

    for segment in segments:
        futures.append(pipeline_seg(st, segment, cl=cl, cfile=cfile,
                                    vys_timeout=vys_timeout))

    return futures  # list of dicts


def pipeline_seg(st, segment, cl=None, cfile=None,
                 vys_timeout=vys_timeout_default):
    """ Submit pipeline processing of a single segment to scheduler.
    Can use distributed client or compute locally.

    Uses distributed resources parameter to control scheduling of GPUs.
    Pipeline produces jobs per DM/dt.
    Returns a dict with values as futures of certain jobs (data, collection).
    """

    logger.info('Building dask for observation {0}, scan {1}, segment {2}.'
                .format(st.metadata.datasetId, st.metadata.scan, segment))

    futures = {}

    mode = 'single' if st.prefs.nthread == 1 else 'multi'
    searchresources = {'MEMORY': 2*st.immem+2*st.vismem,
                       'CORES': st.prefs.nthread}
#    maxlen = 30  # sharing data over imaging chunks crashes
#    imgbytes = maxlen*st.npixx*st.npixy*8/1000.**3
#    searchresources = {'MEMORY': 4*imgbytes,
#                       'CORES': st.prefs.nthread}
    if st.fftmode == 'cuda':
        searchresources['GPU'] = 1

#    datalen = [[(st.readints-st.dmshifts[dmind])//st.dtarr[dtind]
#                for dtind in range(len(st.dtarr))]
#               for dmind in range(len(st.dmarr))]

    if not cl:
        cl = distributed.Client(n_workers=1, threads_per_worker=1)

    # plan, if using fftw
    wisdom = cl.submit(search.set_wisdom, st.npixx, st.npixy,
                       pure=True, resources={'CORES': 1}) if st.fftmode == 'fftw' else None

    data = cl.submit(source.read_segment, st, segment, timeout=vys_timeout,
                     cfile=cfile, pure=True, resources={'MEMORY': 2*st.vismem,
                                                        'CORES': 1})
    futures['data'] = data
    data_prep = cl.submit(source.data_prep, st, data, pure=True,
                          resources={'MEMORY': 2*st.vismem,
                                     'CORES': 1})

    saved = []
    for dmind in range(len(st.dmarr)):
#        delay = cl.submit(util.calc_delay, st.freq, st.freq.max(),
#                          st.dmarr[dmind], st.inttime, pure=True,
#                          resources={'CORES': 1})
#        data_dm = cl.submit(search.dedisperse, data_prep, delay, mode=mode,
#                            pure=True, resources={'MEMORY': 2*st.vismem,
#                                                  'CORES': st.prefs.nthread})

        for dtind in range(len(st.dtarr)):
            saved.append(cl.submit(search.correct_search_thresh, st, segment,
                         data_prep, dmind, dtind, mode=mode, wisdom=wisdom,
                         pure=True, resources=searchresources))

#            data_dmdt = cl.submit(search.dedisperseresample, data_prep, delay,
#                                  st.dtarr[dtind], mode=mode, pure=True,
#                                  resources={'MEMORY': 2*st.vismem,
#                                             'CORES': st.prefs.nthread})

#            fulllen = datalen[dmind][dtind]
#            integrationlist = [list(range(fulllen)[i:i+maxlen])
#                               for i in range(0, fulllen, maxlen)]
#            for integrations in integrationlist:
#            saved.append(cl.submit(search.search_thresh, st, data_dmdt,
#                                   segment, dmind, dtind,
#                                   wisdom=wisdom, pure=True,
#                                   resources=searchresources))

    canddatalist = cl.submit(mergelists, saved, pure=True,
                             resources={'CORES': 1})
    candcollection = cl.submit(candidates.calc_features, canddatalist,
                               pure=True, resources={'CORES': 1})

    futures['candcollection'] = candcollection

    return futures


def mergelists(futlists):
    """ Take list of lists and return single list
    ** TODO: could put logic here to find islands, peaks, etc?
    """

    return [fut for futlist in futlists for fut in futlist]
