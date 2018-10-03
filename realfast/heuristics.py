from __future__ import print_function, division, absolute_import#, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import os.path
import sys
from math import log
from rfpipe import state
import logging
logger = logging.getLogger(__name__)


def reader_memory_available(cl):
    """ Calc memory in use by READERs
    """

    return [vals['memory_limit']-vals['metrics']['memory']
            for vals in itervalues(cl.scheduler_info()['workers'])
            if 'READER' in vals['resources']]


def reader_memory_used(cl):
    """ Calc memory in use by READERs
    """

    return [vals['metrics']['memory']
            for vals in itervalues(cl.scheduler_info()['workers'])
            if 'READER' in vals['resources']]


def spilled_memory(daskdir='.'):
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


def spilled_memory_ok(limit=1.0, daskdir='.'):
    """ Calculate total memory spilled (in GB) by dask distributed.
    """

    spilled = spilled_memory(daskdir)

    if spilled < limit:
        return True
    else:
        logger.info("Spilled memory {0:.1f} GB exceeds limit of {1:.1f}"
                    .format(spilled, limit))
        return False


def state_validates(config=None, inmeta=None, sdmfile=None, sdmscan=None,
                    bdfdir=None, preffile=None, prefsname=None, inprefs={}):
    """ Try to compile state
    """

    try:
        st = state.State(inmeta=inmeta, config=config, preffile=preffile,
                         inprefs=inprefs, name=prefsname, sdmfile=sdmfile,
                         sdmscan=sdmscan, bdfdir=bdfdir, showsummary=False,
                         validate=True)
        return True
    except:
        logger.warn("State did not validate: ", sys.exc_info()[0])
        return False


def reffreq_to_band(reffreqs, edge=5e8):
    """ Given list of reffreqs, return name of band that contains all of them.
    edge defines frequency edge around each nominal band to include.
    """

    nspw = len(reffreqs)
    for band, low, high in [('L', 1e9, 2e9), ('S', 2e9, 4e9),
                            ('C', 4e9, 8e9), ('X', 8e9, 12e9),
                            ('Ku', 12e9, 18e9), ('K', 18e9, 26.5e9),
                            ('Ka', 26.5e9, 30e9), ('Q', 40e9, 50e9)]:
        reffreq_inband = [reffreq for reffreq in reffreqs
                          if ((reffreq >= low-edge) and (reffreq < high+edge))]
        if len(reffreq_inband) == nspw:
            return band

    return None


def is_nrao_default(inmeta):
    """ Parses metadata to determine if it is consistent with NRAO default
    correlator mode.
    """

    nspw = len(inmeta['spw_orig'])
    if nspw != 16:
        logger.info("NRAO default fail: {0} spw".format(nspw))
        return False
    else:
        logger.info("NRAO default pass: 16 spw")

    band = reffreq_to_band(inmeta['spw_reffreq'])
    if band is None:
        logger.info("NRAO default fail: reffreqs not in just band {0} "
                    .format(inmeta['spw_reffreq']))
        return False
    else:
        logger.info("NRAO default pass: All {0} spw are in {1} band"
                    .format(nspw, band))

    if not all([nchan == 64 for nchan in inmeta['spw_nchan']]):
        logger.info("NRAO default fail: not all spw have 64 chans {0} "
                    .format(inmeta['spw_nchan']))
        False
    else:
        logger.info("NRAO default pass: all spw have 64 channels")
        nchan = 64

    if len(inmeta['pols_orig']) != 4:
        logger.info("NRAO default fail: {0} pols".format(inmeta['pols_orig']))
        return False
    else:
        logger.info("NRAO default pass: Full pol")

    bandwidth = sum([nchan * chansize for chansize in inmeta['spw_chansize']])
    if band == 'L' and bandwidth != 1024000000.0:
        logger.info("NRAO default fail: band {0} has bandwidth {1}"
                    .format(band, bandwidth))
        return False
    elif band == 'S' and bandwidth != 2048000000.0:
        logger.info("NRAO default fail: band {0} has bandwidth {1}"
                    .format(band, bandwidth))
        return False
    elif band == 'C' and bandwidth != 2048000000.0:
        logger.info("NRAO default fail: band {0} has bandwidth {1}"
                    .format(band, bandwidth))
        return False
    elif band == 'X' and bandwidth != 2048000000.0:
        logger.info("NRAO default fail: band {0} has bandwidth {1}"
                    .format(band, bandwidth))
        return False
    else:
        logger.info("NRAO default pass: bandwidth {0} for band {1}"
                    .format(bandwidth, band))

    return True


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
