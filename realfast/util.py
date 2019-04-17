from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import numpy as np
import glob
import os.path
import subprocess
from rfpipe import candidates, fileLock
from realfast import elastic, mcaf_servers

import logging
logger = logging.getLogger(__name__)

_candplot_dir = 'claw@nmpost-master:/lustre/aoc/projects/fasttransients/realfast/plots'
_candplot_url_prefix = 'http://realfast.nrao.edu/plots'


def indexcands_and_plots(cc, scanId, tags, indexprefix, workdir):
    """ Wraps indexcands, makesummaryplot, and moveplots calls.
    """

    if len(cc):
        nc = elastic.indexcands(cc, scanId, tags=tags,
                                url_prefix=_candplot_url_prefix,
                                indexprefix=indexprefix)

        # TODO: makesumaryplot logs cands in all segments
        # this is confusing when only one segment being handled here
        msp = makesummaryplot(workdir, scanId)
        moveplots(cc, scanId, destination='{0}/{1}'.format(_candplot_dir,
                                                           indexprefix))
    else:
        nc = 0
        msp = 0

    if nc or msp:
        logger.info('Indexed {0} cands to {1} and moved plots and '
                    'summarized {2} to {3} for scanId {4}'
                    .format(nc, indexprefix+'cands', msp, _candplot_dir,
                            scanId))
    else:
        logger.info('No candidates or plots found.')


def makesummaryplot(workdir, scanId):
    """ Create summary plot for a given scanId and move it
    """

    candsfile = '{0}/cands_{1}.pkl'.format(workdir, scanId)
    ncands = candidates.makesummaryplot(candsfile)
    return ncands


def moveplots(candcollection, scanId, destination=_candplot_dir):
    """ For given fileroot, move candidate plots to public location
    """

    workdir = candcollection.prefs.workdir
#    segment = candcollection.segment

    logger.info("Moving plots for scanId {0} to {1}"
                .format(scanId, destination))

#    nplots = 0
#    candplots = glob.glob('{0}/cands_{1}_seg{2}-*.png'
#                          .format(workdir, scanId, segment))
#    for candplot in candplots:
#        success = rsync(candplot, destination)
#        if not success:  # make robust to a failure
#           success = rsync(candplot, destination)
#
#        nplots += success

    # move summary plot too
#    summaryplot = '{0}/cands_{1}.html'.format(workdir, scanId)
#    summaryplotdest = os.path.join(destination, os.path.basename(summaryplot))
#    if os.path.exists(summaryplot):
#        success = rsync(summaryplot, summaryplotdest)
#        if not success:
#            success = rsync(summaryplot, summaryplotdest)
#    else:
#        logger.warn("No summary plot {0} found".format(summaryplot))

    args = ["rsync", "-av", "--include", "cands_{0}*png".format(scanId),
            "--include", "cands_{0}*.html".format(scanId), "--exclude", "*",
            workdir, destination]

    subprocess.call(args)


def indexcandsfile(candsfile, indexprefix, tags=None):
    """ Use candsfile to index cands, scans, prefs, mocks, and noises.
    Should produce identical index results as real-time operation.
    """

    for cc in candidates.iter_cands(candsfile):
        st = cc.state
        scanId = st.metadata.scanId
        workdir = st.prefs.workdir
        mocks = st.prefs.simulated_transient

        elastic.indexscan(inmeta=st.metadata, preferences=st.prefs,
                          indexprefix=indexprefix)
        indexcands_and_plots(cc, scanId, tags, indexprefix, workdir)
        elastic.indexnoises(cc.state.noisefile, scanId,
                            indexprefix=indexprefix)
        if mocks is not None:
            elastic.indexmock(scanId, mocks, indexprefix=indexprefix)


def createproducts(candcollection, data, indexprefix=None,
                   savebdfdir='/lustre/evla/wcbe/data/realfast/'):
    """ Create SDMs and BDFs for a given candcollection (time segment).
    Takes data future and calls data only if windows found to cut.
    This uses the mcaf_servers module, which calls the sdm builder server.
    Currently BDFs are moved to no_archive lustre area by default.
    """

    from rfpipe import calibration
    from distributed import Future

    if isinstance(candcollection, Future):
        candcollection = candcollection.result()
    if isinstance(data, Future):
        data = data.result()

    assert isinstance(data, np.ndarray) and data.dtype == 'complex64'

    logger.info("Creating an SDM for {0}, segment {1}, with {2} candidates"
                .format(candcollection.metadata.scanId, candcollection.segment,
                        len(candcollection)))

    if len(candcollection.array) == 0:
        logger.info('No candidates to generate products for.')
        return []

    metadata = candcollection.metadata
    segment = candcollection.segment
    if not isinstance(segment, int):
        logger.warning("Cannot get unique segment from candcollection")

    st = candcollection.state

    candranges = gencandranges(candcollection)  # finds time windows to save from segment
    logger.info('Getting data for candidate time ranges {0} in segment {1}.'
                .format(candranges, segment))

    ninttot, nbl, nchantot, npol = data.shape
    nchan = metadata.nchan_orig//metadata.nspw_orig
    nspw = metadata.nspw_orig

    sdmlocs = []
    # make sdm for each unique time range (e.g., segment)
    for (startTime, endTime) in set(candranges):
        i = (86400*(startTime-st.segmenttimes[segment][0])/metadata.inttime).astype(int)
        nint = np.round(86400*(endTime-startTime)/metadata.inttime, 1).astype(int)
        logger.info("Cutting {0} ints from int {1} for candidate at {2} in segment {3}"
                    .format(nint, i, startTime, segment))
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
            # update index to link to new sdm
            if indexprefix is not None:
                candIds = elastic.candid(cc=candcollection)
                for Id in candIds:
                    elastic.update_field(indexprefix+'cands', 'sdmname',
                                         sdmloc, Id=Id)

            sdmlocs.append(sdmloc)
            logger.info("Created new SDMs at: {0}".format(sdmloc))
            # TODO: migrate bdfdir to newsdmloc once ingest tool is ready
            mcaf_servers.makebdf(startTime, endTime, metadata, data_cut,
                                 bdfdir=savebdfdir)
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


def data_logger(st, segment, data):
    """ Function that inspects read data and writes results to file.
    """

    filename = os.path.join(st.prefs.workdir,
                            "data_" + st.fileroot + ".txt")

    t0 = st.segmenttimes[segment][0]
    timearr = ','.join((t0+st.metadata.inttime*np.arange(st.readints))
                       .astype(str))

    if data.ndim == 4:
        results = ','.join(data.mean(axis=3).mean(axis=2).any(axis=1)
                           .astype(str))
    else:
        results = 'None'

    try:
        with fileLock.FileLock(filename, timeout=60):
            with open(filename, "a") as fp:
                fp.write("{0}: {1} {2} {3}\n".format(segment, t0, timearr,
                                                     results))
    except fileLock.FileLock.FileLockException:
        logger.warn("data_logger on segment {0} failed to write due to file timeout"
                    .format(segment))


def data_logging_report(filename):
    """ Read and summarize data logging file
    """

    with open(filename, 'r') as fp:
        for line in fp.readlines():
            segment, starttime, dts, filled = line.split(' ')
            segment = int(segment.rstrip(':'))
            starttime = float(starttime)
            dts = [float(dt) for dt in dts.split(',')]
            filled = [fill.rstrip() == "True" for fill in filled.split(',')]
            if len(filled) > 1:
                filled = np.array(filled, dtype=int)
                logger.info("Segment {0}: {1}/{2} missing"
                            .format(segment, len(filled)-filled.sum(),
                                    len(filled)))
                logger.info("{0}".format(filled))
            else:
                logger.info("Segment {0}: no data".format(segment))


def gencandranges(candcollection):
    """ Given a candcollection, define a list of candidate time ranges.
    Currently saves the whole segment.
    """

    segment = candcollection.segment
    st = candcollection.state

    # save whole segment
    return [(st.segmenttimes[segment][0], st.segmenttimes[segment][1])]


def rsync(original, new):
    """ Uses subprocess.call to rsync from 'filename' to 'new'
    If new is directory, copies original in.
    If new is new file, copies original to that name.
    """

    assert os.path.exists(original), 'Need original file!'
    res = subprocess.call(["rsync", "--timeout", "30", "-a", original.rstrip('/'), new.rstrip('/')])

    return int(res == 0)
