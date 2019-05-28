from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import numpy as np
import os
import shutil
import subprocess
from astropy import time
from time import sleep
from realfast import elastic, mcaf_servers
import distributed

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
        workdir = cc.prefs.workdir + '/'
        moveplots(workdir, scanId, destination='{0}/{1}'.format(_candplot_dir,
                                                                indexprefix))
    else:
        nc = 0
        msp = 0
        if len(cc.array) > 0:
            logger.warn('CandCollection in segment {0} is empty. '
                        '{1} realtime detections exceeded maximum'
                        .format(cc.segment, cc.array.ncands))

    if nc or msp:
        logger.info('Indexed {0} cands to {1} and moved plots and '
                    'summarized {2} to {3} for scanId {4}'
                    .format(nc, indexprefix+'cands', msp, _candplot_dir,
                            scanId))
    else:
        logger.info('No candidates or plots found.')


def makesummaryplot(workdir, scanId):
    """ Create summary plot for a given scanId and move it
    TODO: allow candcollection to be passed instead of assuming pkl flie
    """

    from rfpipe import candidates

    candsfile = '{0}/cands_{1}.pkl'.format(workdir, scanId)
    if os.path.exists(candsfile):
        ncands = candidates.makesummaryplot(candsfile)
    else:
        logger.warn("No candsfile found. No summary plot made.")
        ncands = None
    return ncands


def moveplots(workdir, scanId, destination=_candplot_dir):
    """ For given fileroot, move candidate plots to public location
    """

    logger.info("Moving scanId {0} plots from {1} to {2}"
                .format(scanId, workdir, destination))

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

    from rfpipe import candidates

    for cc in candidates.iter_cands(candsfile):
        st = cc.state
        scanId = st.metadata.scanId
        workdir = st.prefs.workdir
        mocks = st.prefs.simulated_transient

        elastic.indexscan(inmeta=st.metadata, preferences=st.prefs,
                          indexprefix=indexprefix)
        indexcands_and_plots(cc, scanId, tags, indexprefix, workdir)
        elastic.indexnoises(scanId, noisefile=cc.state.noisefile,
                            indexprefix=indexprefix)
        if mocks is not None:
            elastic.indexmock(scanId, mocks, indexprefix=indexprefix)


def calc_and_indexnoises(st, segment, data, indexprefix='new'):
    """ Wraps calculation and indexing functions.
    Should get calibrated data as input.
    """

    from rfpipe.util import calc_noise

    noises = calc_noise(st, segment, data)
    elastic.indexnoises(st.metadata.scanId, noises=noises,
                        indexprefix=indexprefix)


def createproducts(candcollection, data, indexprefix=None,
                   savebdfdir='/lustre/evla/wcbe/data/realfast/'):
    """ Create SDMs and BDFs for a given candcollection (time segment).
    Takes data future and calls data only if windows found to cut.
    This uses the mcaf_servers module, which calls the sdm builder server.
    Currently BDFs are moved to no_archive lustre area by default.
    """

    if isinstance(candcollection, distributed.Future):
        candcollection = candcollection.result()

    if len(candcollection) == 0:
        logger.info('No candidates to generate products for.')
        return []

    if isinstance(data, distributed.Future):
        data = data.result()

    assert isinstance(data, np.ndarray) and data.dtype == 'complex64'

    logger.info("Creating an SDM for {0}, segment {1}, with {2} candidates"
                .format(candcollection.metadata.scanId, candcollection.segment,
                        len(candcollection)))

    wait = candcollection.metadata.endtime_mjd + 10/(24*3600)  # end+10s
    now = time.Time.now().mjd
    if now < wait:
        logger.info("Waiting until {0} for ScanId {1} to complete"
                    .format(wait, candcollection.metadata.scanId))

        while now < wait:
            sleep(1)
            now = time.Time.now().mjd

    metadata = candcollection.metadata
    segment = candcollection.segment
    if not isinstance(segment, int):
        logger.warning("Cannot get unique segment from candcollection")

    st = candcollection.state

    candranges = gencandranges(candcollection)  # finds time windows to save
    calScanTime = candcollection.soltime  # solution saved during search
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

# now retrieved from candcollection
#        calScanTime = np.unique(calibration.getsols(st)['mjd'])
#        if len(calScanTime) > 1:
#            logger.warn("Using first of multiple cal times: {0}."
#                        .format(calScanTime))
#        calScanTime = calScanTime[0]

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


def classify_candidates(cc, indexprefix='new'):
    """ Submit canddata object to node with fetch model ready
    """

    index = indexprefix + 'cands'

    try:
        if len(cc.canddata):
            logger.info("Running fetch classifier on {0} candidates for scanId {1}, "
                        "segment {2}"
                        .format(len(cc.canddata), cc.metadata.scanId, cc.segment))

            for cd in cc.canddata:
                frbprob = candidates.cd_to_fetch(cd, classify=True)
                elastic.update_field(index, 'frbprob', frbprob, Id=cd.candid)
        else:
            logger.info("No candidates to classify for scanId {0}, segment {1}."
                        .format(cc.metadata.scanId, cc.segment))
    except AttributeError:
        logger.info("CandCollection has no canddata attached. Not classifying.")


def get_sdmname(candcollection):
    """ Use candcollection to get name of SDM output by the sdmbuilder
    """

    datasetId = candcollection.metadata.datasetId
    uid = get_uid(candcollection)
    outputDatasetId = 'realfast_{0}_{1}'.format(datasetId, uid.rsplit('/')[-1])

    return outputDatasetId


def get_uid(candcollection):
    """ Get unix time appropriate for bdf naming
    Expects one segment per candcollection and one range per segment.
    """

    candranges = gencandranges(candcollection)
    assert len(candranges) == 1, "Assuming 1 candrange per candcollection and/or segment"

    startTime, endTime = candranges[0]

    uid = ('uid:///evla/realfastbdf/{0}'
           .format(int(time.Time(startTime, format='mjd').unix*1e3)))

    return uid


def assemble_sdmbdf(candcollection, sdmloc='/home/mctest/evla/mcaf/workspace/',
                    bdfdir='/lustre/evla/wcbe/data/realfast/'):
    """ Use candcollection to assemble cutout SDM and BDF from file system at VLA
    into a viable SDM.
    """

    destination = '.'
    sdm0 = get_sdmname(candcollection)
    bdf0 = get_uid(candcollection).replace(':', '_').replace('/', '_')
    newsdm = os.path.join(destination, sdm0)
    shutil.copytree(os.path.join(sdmloc, sdm0), newsdm)

    bdfdestination = os.path.join(newsdm, 'ASDMBinary')
    os.mkdir(bdfdestination)
    shutil.copy(os.path.join(bdfdir, bdf0), os.path.join(bdfdestination, bdf0))

    logger.info("Assembled SDM/BDF at {0}".format(newsdm))

    return newsdm

def runingest(sdms):
    """ Call archive tool or move data to trigger archiving of sdms.
    This function will ultimately be triggered by candidate portal.
    """

    NotImplementedError
#    /users/vlapipe/workflows/test/bin/ingest -m -p /home/mctest/evla/mcaf/workspace --file 


def update_slack(channel, message):
    """ Use slack web API to send message to realfastvla slack
    channel should start with '#' and be existing channel.
    API token set for realfast user on cluster.
    """

    import os
    import slack

    client = slack.WebClient(token=os.environ['SLACK_API_TOKEN'])

    response = client.chat_postMessage(
        channel=channel,
        text=message)
    assert response["ok"]
    assert response["message"]["text"] == message


def data_logger(st, segment, data):
    """ Function that inspects read data and writes results to file.
    """

    from rfpipe import fileLock

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


def initialize_worker():
    from rfpipe import search, state, metadata

    search.set_wisdom(512)
    st = state.State(inmeta=metadata.mock_metadata(0,0.001, 27, 16, 16*32, 4, 5e4, datasource='sim'))


def rsync(original, new):
    """ Uses subprocess.call to rsync from 'filename' to 'new'
    If new is directory, copies original in.
    If new is new file, copies original to that name.
    """

    assert os.path.exists(original), 'Need original file!'
    res = subprocess.call(["rsync", "--timeout", "30", "-a", original.rstrip('/'), new.rstrip('/')])

    return int(res == 0)
