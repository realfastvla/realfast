from __future__ import print_function, division, absolute_import#, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

from rfpipe import state
import os.path
from math import log
import logging
logger = logging.getLogger(__name__)


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


def valid_telcalfile(st):
    """ Test whether telcalfile exists at the moment.
    Note: telcalfile may appear later.
    """

    if os.path.exists(st.gainfile) and os.path.isfile(st.gainfile):
        return True
    else:
        return False


def state_compiles(config=None, inmeta=None, sdmfile=None, sdmscan=None,
                   bdfdir=None, preffile=None, inprefs=None):
    """ Try to compile state
    """

    try:
        st = state.State(inmeta=inmeta, config=config, preffile=preffile,
                         inprefs=inprefs, sdmfile=sdmfile, sdmscan=sdmscan,
                         bdfdir=bdfdir, showsummary=False)
        return True
    except:
        return False


def total_images_searched(st):
    """ Number of images formed (trials) in all segments, dms, dts.
    """

    si = 0
    for segment in range(st.nsegment):
        for dmind in range(len(st.dmarr)):
            for dtind in range(len(st.dtarr)):
                si += len(st.get_search_ints(segment, dmind, dtind))
    return si


def total_compute_time(st):
    """ Uses a simple model for total GPU compute time based on profiling
    GPU time per trial (incl data in, amortized over many dm/dt)
    No distributed data movement time included.
    2.3e-4 s (512x512)
    6.1e-4 s (1024x1024)
    1.2e-3 s (2048x2048)
    3.8e-3 s (4096x4096)
    """

    time_ref = 2.3e-4
    npix_ref = 512

    si = total_images_searched(st)
    npix = (st.npixx+st.npixy)/2

    return si * time_ref * npix*log(npix)/(npix_ref*log(npix_ref))


def total_read_memory(st):
    """ Memory read, including extra memory at segment boundaries.
    """

    return st.nsegment*st.vismem