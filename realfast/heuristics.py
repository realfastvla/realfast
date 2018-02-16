from __future__ import print_function, division, absolute_import#, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

from rfpipe import state
import os.path
from math import log
import logging
logger = logging.getLogger(__name__)


def reader_memory_available(cl):
    """ Calc memory in use by READERs
    """

    return [vals['memory_limit']-vals['memory']
            for vals in itervalues(cl.scheduler_info()['workers'])
            if 'READER' in vals['resources']]


def reader_memory_used(cl):
    """ Calc memory in use by READERs
    """

    return [vals['memory']
            for vals in itervalues(cl.scheduler_info()['workers'])
            if 'READER' in vals['resources']]


def spilled_memory(daskdir='/lustre/evla/test/realfast/dask-worker-space'):
    """ How much memory has been spilled by dask/distributed?
    """

    spilled = 0
    for dirpath, dirnames, filenames in os.walk(daskdir):
        for filename in filenames:
            try:
                spilled += os.path.getsize(os.path.join(dirpath, filename))/1024.**3
            except OSError:
                try:
                    spilled += os.path.getsize(os.path.join(dirpath, filename))/1024.**3
                except OSError:
                    logger.warn("Could not get size of spilled file. Skipping.")

    return spilled


def reader_memory_ok(cl, memory_required):
    """ Does any READER worker have enough memory?
    memory_required is the size of the read in bytes
    """

    for worker_memory in reader_memory_available(cl):
        if worker_memory > memory_required:
            return True

    logger.info("No worker found with required memory of {0} GB"
                .format(memory_required/1e9))

    return False


def readertotal_memory_ok(cl, memory_limit):
    """ Is total READER memory usage too high?
    memory_limit is total memory used in bytes
    """

    if memory_limit is not None:
        total = sum(reader_memory_used(cl))

        if total > memory_limit:
            logger.info("Total of {0} GB in use. Exceeds limit of {1} GB."
                        .format(total/1e9, memory_limit/1e9))

        return total < memory_limit
    else:
        return True


def spilled_memory_ok(limit=1.0, daskdir='/lustre/evla/test/realfast/dask-worker-space'):
    """ Calculate total memory spilled (in GB) by dask distributed.
    """

    spilled = spilled_memory(daskdir)

    if spilled < limit:
        return True
    else:
        logger.info("Spilled memory {0:.1f} GB exceeds limit of {1:.1f}"
                    .format(spilled, limit))
        return False


def valid_telcalfile(st):
    """ Test whether telcalfile exists at the moment.
    Note: telcalfile may appear later.
    """

    if os.path.exists(st.gainfile) and os.path.isfile(st.gainfile):
        return True
    else:
        return False


def state_compiles(config=None, inmeta=None, sdmfile=None, sdmscan=None,
                   bdfdir=None, preffile=None, inprefs={}):
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
    """ Uses a simple model for total GPU compute time (in sec) based on profiling.
    Models the GPU time per trial (incl data in, amortized over many dm/dt).
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


def total_memory_read(st):
    """ Memory read (in GB) including overlapping read at segment boundaries.
    """

    return st.nsegment*st.vismem
