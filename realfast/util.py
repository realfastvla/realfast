from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

from math import floor
import os
import shutil
import subprocess
import numpy as np
from astropy import time
from time import sleep
from realfast import elastic, mcaf_servers
import distributed
from elasticsearch import NotFoundError

import logging
logger = logging.getLogger(__name__)
logger.setLevel(20)

_candplot_dir = 'claw@nmpost-master:/lustre/aoc/projects/fasttransients/realfast/plots'
_candplot_url_prefix = 'http://realfast.nrao.edu/plots'


def indexcands_and_plots(cc, scanId, tags, indexprefix, workdir):
    """ Wraps indexcands, makesummaryplot, and moveplots calls.
    """

    from rfpipe import candidates

    if len(cc):
        logger.info("Indexing candidates")
        nc = elastic.indexcands(cc, scanId, tags=tags,
                                url_prefix=_candplot_url_prefix,
                                indexprefix=indexprefix)

        assoc = find_bad(cc)  # find false positives
        if assoc is not None:
            for i, candId in enumerate(cc.candids):
                if assoc[i]:
                    # set a tag to indicate false positive
                    status = elastic.add_tag(indexprefix, candId, 'caseyjlaw',
                                             'astrophysical,delete')
                    if not status:
                        logger.warn("CandId {0} not found in {1}"
                                    .format(candId, indexprefix))

        # TODO: makesumaryplot logs cands in all segments
        # this is confusing when only one segment being handled here
#        msp = makesummaryplot(workdir, scanId)
        candsfile = cc.state.candsfile
        msp = candidates.makesummaryplot(candsfile=candsfile)
        # moving of plots managed by rsync on rfnode021 now
#        workdir = cc.prefs.workdir + '/'
#        moveplots(workdir, scanId, destination='{0}/{1}'.format(_candplot_dir,
#                                                                indexprefix))
    else:
        nc = 0
        msp = 0
        if len(cc.array) > 0:
            logger.warn('CandCollection in segment {0} is empty. '
                        '{1} realtime detections exceeded maximum'
                        .format(cc.segment, cc.array.ncands))

    if nc or msp:
        logger.info('Indexed {0} new cands to {1}, moved plots, and '
                    'summarized {2} total cands to {3} for scanId {4}'
                    .format(nc, indexprefix+'cands', msp, _candplot_dir,
                            scanId))
    else:
        logger.info('No candidates or plots found.')

    return cc


def send_voevent(cc, dm='FRB', dt=None, snrtot=None, frbprobt=None, mode='max', destination=None):
    """ Runs make_voevent for some selection of candidates and optionall sends them.
    mode can be 'max' or 'all', which selects whether to make/send for all cands
    or just max of snrtot.
    """

    from rfpipe import candidates
    assert mode in ['max', 'all']

    if isinstance(cc, distributed.Future):
        cc = cc.result()

    voeventdir = '/lustre/aoc/projects/fasttransients/realfast/voevents/'
    assoc = find_bad(cc)  # find false positives

    if assoc is not None:
        # select those without assoc
        cclist = [cc0 for (i, cc0) in enumerate(cc) if not assoc[i]]
        if len(cclist):
            cc = select_cc(sum(cclist), dm=dm, dt=dt, snrtot=snrtot, frbprobt=frbprobt)
        else:
            cc = []
    else:
        cc = select_cc(cc, dm=dm, dt=dt, snrtot=snrtot, frbprobt=frbprobt)

    if len(cc):
        if mode == 'max':
            # define max snr for good cands
            cc0 = cc[np.where(cc.snrtot == max(cc.snrtot))[0][0]]
            logger.info('Making VOEvent xml file for max snrtot')
        else:
            cc0 = cc
            logger.info('Making {0} VOEvent xml files'.format(len(cc0)))

        if destination is None:
            role = 'test'
        else:
            role = 'observation'
        outnames = candidates.make_voevent(cc0, role=role)

        # send to destination
        if destination is not None:
            logger.info("Sending voevent(s) to {0}".format(destination))
            for outname in outnames:
                success = rsync(outname, 'claw@nmpost-master:' + voeventdir)
                if success:
                    outname0 = voeventdir + outname.split('/')[-1]
                    subprocess.call(['ssh', 'claw@nmpost-master', 'conda activate rfs; comet-sendvo -h {0} -f {1}'.format(destination, outname0)])
                else:
                    logger.warn("rsync of voevent xml file failed.")
        else:
            logger.info("Not sending voevent(s)")
    else:
        logger.info("No candidates meet criteria for voevent generation.")


def select_cc(cc, snrtot=None, dm=None, dt=None, frbprobt=None, dm_halo=10, timeout=300):
    """ Filter candcollections based on candidate properties.
    If snrtot and dm are set, candidates must have larger values.
    DM can be float in pc/cm3 or 'FRB', which uses NE2001 plus halo
    model of YT2020. Uses implementation in FRB/ne2001.
    frbprob sets threshold on fetch classifier output.
    timeout is time in seconds before giving up on fetch classification.
    Returns new subset cc that passes selection criteria.
    """

    from rfpipe import candidates
    from astropy import coordinates, units
    from ne2001 import ne_io, density

    if frbprobt is None:
        frbprobt = 0.

    sel = [True]*len(cc)

    if len(cc):
        # snr selection
        if snrtot is not None:
            sel *= cc.snrtot > snrtot

        # dm selection
        dmt = 0.
        if isinstance(dm, str):
            if dm.upper() == "FRB":  # calc DM threshold per candidate
                ne = density.ElectronDensity(**ne_io.Params())
                coords = get_skycoord(cc)
                ls, bs = coords.galactic.l, coords.galactic.b
                dmt = [ne.DM(l, b, 20.).value + dm_halo for (l, b) in zip(ls, bs)]
            else:
                logger.warn("dm string ({0}) not recognized".format(dm))
        elif isinstance(dm, float) or isinstance(dm, int):  # single DM threshold
            dmt = dm
        sel *= cc.canddm > dmt

        # select by max width
        if dt is not None:
            sel *= cc.canddt < dt
        
        # query portal to get frbprob for interesting cands
        if (frbprobt > 0.) and (True in sel):
            probset = sel == False  # cands that need frbprob to be set
            t0 = time.Time.now().mjd
            while time.Time.now().mjd-t0 < timeout/(24*3600):
                for i, candId in enumerate(cc.candids):
                    if (sel[i] == True) and (probset[i] == False):
                        try:
                            doc = elastic.get_doc('newcands', candId)
                        except NotFoundError:
                            continue

                        if 'frbprob' in doc['_source']:
                            frbprob = doc['_source']['frbprob']
                            if frbprob > frbprobt:
                                sel[i] = True
                                probset[i] = True
                            else:
                                sel[i] = False
                                probset[i] = True

                if all(probset):
                    break
                else:
                    count = len(np.where(probset)[0])
                    logger.info("{0} of {1} candidates need an frbprob ({2}s timeout)".format(len(sel)-count, len(sel), timeout))
                    sleep(10)

            if time.Time.now().mjd-t0 > timeout/(24*3600):
                logger.warn("Timed out of frbprob queries. Not selecting {0} cands without an frbprob".format(len(np.where(probset == False)[0])))
                sel *= probset  # do not select ones without frbprob, if required

        sel = np.where(sel)[0]
        if len(sel):
            cc0 = sum([cc[i] for i in sel])
        else:
            cc0 = []
    else:
        cc0 = cc

    logger.info("Selecting {0} of {1} candidates for dm={2}, dt={3}, snrtot={4}, frbprobt={5}".format(len(sel), len(cc), dm, dt, snrtot, frbprobt))

    return cc0


def get_skycoord(cc):
    """ Convert candidate coordinates in to SkyCoord (catalog).
    """

    from rfpipe import candidates
    from astropy import coordinates, units

    if len(cc):
        st = cc.state
        segment = cc.segment
        pc0 = st.get_pc(segment)
        ra_ctr, dec_ctr = st.get_radec(pc=pc0)
        l1 = cc.candl
        m1 = cc.candm
        ra, dec = candidates.source_location(ra_ctr, dec_ctr, l1, m1, format='degfloat')
        coords = coordinates.SkyCoord(ra, dec, unit=units.deg)
    else:
        coords = None

    return coords


def find_bad(cc, nvss_radius=5, nvss_flux=400, catfile='nvss_astropy.pkl'):
    """ Identify candidates that are likely false positives.
    Major check is for bright NVSS sources during VLASS.
    Return boolean for each one, where true means a problematic source.
    nvss_radius (arcsec) and nvss_flux (mJy) are arguments.
    """

    from astropy import units
    import pickle

    workdir = cc.prefs.workdir + '/'

    if not len(cc):
        return None

    # if using OTF (mostly)
    if any([reject in cc.metadata.datasetId for reject in ['VLASS', 'TOTF', 'TSKY']]):
        if os.path.exists(workdir + catfile):
            logger.info("Loading NVSS catfile")
            with open(workdir + catfile, 'rb') as pkl:
                catalog = pickle.load(pkl)
                fluxes = pickle.load(pkl)
        else:
            logger.warn("No catfile {0} found in workdir {1}".format(catfile, workdir))
            return None

        assoc = []
        coords = get_skycoord(cc)
        if coords is not None:
            logger.info("Comparing SkyCoord for candidates to NVSS.")
            for coord in coords:
                ind, sep2, sep3 = coord.match_to_catalog_sky(catalog)
                if sep2.value < nvss_radius and fluxes[ind] > nvss_flux:
                    assoc.append(True)
                else:
                    assoc.append(False)

        return assoc
    else:  # none bad otherwise
        return [False]*len(cc)


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

        if os.path.exists(cc.state.noisefile):
            elastic.indexnoises(scanId, noisefile=cc.state.noisefile,
                                indexprefix=indexprefix)

        if mocks is not None:
            elastic.indexmock(scanId, mocks, indexprefix=indexprefix)


def calc_and_indexnoises(st, segment, data, indexprefix='new'):
    """ Wraps calculation and indexing functions.
    Should get calibrated data as input.
    Checks that scanId is indexed before indexing noise.
    """

    from rfpipe.util import calc_noise

    noises = calc_noise(st, segment, data)
    scindex = indexprefix+'scans'
    try:
        doc = elastic.get_doc(scindex, st.metadata.scanId)
        elastic.indexnoises(st.metadata.scanId, noises=noises,
                            indexprefix=indexprefix)
    except NotFoundError:
        logger.warn("scanId {0} not found in {1}. Not indexing noise estimate."
                    .format(st.metadata.scanId, scindex))


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
    else:
        logger.info('Generating products for {0} candidates in this segment.'.format(len(candcollection)))

    if isinstance(data, distributed.Future):
        logger.info("Calling data in...")
        data = data.result()

    assert isinstance(data, np.ndarray) and data.dtype == 'complex64'

    sdmlocs = []

    assoc = find_bad(candcollection)  # find false positives
    if assoc is not None:
        if all(assoc):
            logger.info("All candidates fail find_bad. Skipping createproducts.")
            return sdmlocs
        else:
            logger.info("Not all candidates fail find_bad.")

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
    annotation = cc_to_annotation(candcollection, run_QA_query=True)

    ninttot, nbl, nchantot, npol = data.shape
    nchan = metadata.nchan_orig//metadata.nspw_orig
    nspw = metadata.nspw_orig

    # if otf, then phase shift data to central phase center
    if st.otfcorrections is not None:
        from rfpipe.source import apply_otfcorrections
        apply_otfcorrections(st, segment, data, raw=True)
        # TODO: also correct metadata in output SDM for new phase center

    # make sdm for each unique time range (e.g., segment)
    for (startTime, endTime) in set(candranges):
        nint = floor(86400*(endTime-startTime)/metadata.inttime)
        logger.info("Cutting {0} ints for candidate at {1} in segment {2}."
                    .format(nint, startTime, segment))
        logger.info("Input shape {0}".format(data.shape))
        data_cut = data[:nint]
        logger.info("Cut to {0} and reshaping to {1}".format(nint, (nint, nbl, nspw, 1, nchan, npol)))
        data_cut = data_cut.reshape(nint, nbl, nspw, 1, nchan, npol)

        logger.info("Creating SDM")
        try:
            sdmloc = mcaf_servers.makesdm(startTime, endTime, metadata.datasetId,
                                          data_cut, calScanTime,
                                          annotation=annotation)
        except HTTPError:
            logger.warn("HTTPError in call to mcaf_server.makesdm for {0}".format(metadata.datasetId))
            sdmloc = None

        if sdmloc is not None:
            sdmpath = os.path.dirname(sdmloc)
            sdmloc = os.path.basename(sdmloc)  # ignore internal mcaf path from here on
            logger.info("Created new SDM in {0} called {1}".format(sdmpath, sdmloc))
            sdmlocs.append(sdmloc)

            # update index to link to new sdm
            if indexprefix is not None:
                candIds = elastic.candid(cc=candcollection)
                for Id in candIds:
                    try:
                        logger.info("Updating index {0}, Id {1}, with sdmname {2}".format(indexprefix+'cands', Id, sdmloc))
                        elastic.update_field(indexprefix+'cands', 'sdmname',
                                             sdmloc, Id=Id)
                    except NotFoundError as exc:
                        logger.warn("elasticsearch cannot find Id {0} in index {1}. Exception: {2}".format(Id, indexprefix+'cands', exc))
                        
            # TODO: migrate bdfdir to newsdmloc once ingest tool is ready
            logger.info("Making bdf for time {0}-{1}".format(startTime, endTime))
            mcaf_servers.makebdf(startTime, endTime, metadata, data_cut,
                                 bdfdir=savebdfdir)
        else:
            logger.warn("No sdm/bdf made for {0} with start/end time {1}-{2}"
                        .format(metadata.datasetId, startTime, endTime))

    return sdmlocs


def cc_to_annotation(cc0, run_QA_query=True, indexprefix='new', timeout=60):
    """ Takes candcollection and returns dict to be passed to sdmbuilder.
    Dict has standard fields to fill annotations table for archiving queries.
    mode can be 'dict' to return single dict at max snrtot or 'list' to return list of dicts.
    run_QA_query will query portal for values to fill QA fields with frbprob timeout.
    indexprefix used for QA query.
    """

    from rfpipe import candidates

    # fixed in cc
    maxsnr = cc0.snrtot.max()
    ind = np.where(cc0.snrtot == maxsnr)[0][0]
    cc = cc0[ind]
    candId = cc.candids[0]  # peak candId
    st = cc.state
    segment = cc.segment

    uvres = st.uvres
    npix = min(st.npixx, st.npixy)  # estimate worst loc
    pixel_sec = np.degrees(1/(uvres*npix))*3600
    dmarr = st.dmarr
    pc0 = st.get_pc(segment)
    ra_ctr, dec_ctr = st.get_radec(pc=pc0)
    scanid = cc.metadata.scanId
    l1 = cc.candl
    m1 = cc.candm
    ra, dec = candidates.source_location(ra_ctr, dec_ctr, l1, m1, format='hourstr')
    candids = ','.join(['{0}_seg{1}-i{2}-dm{3}-dt{4}'.format(scanid, segment, integration, dmind, dtind) for segment, integration, dmind, dtind, beamnum in cc0.locs])

    label = None
    zf = None
    vnoise = None
    inoise = None
    if run_QA_query:
        Ids = [Id for Id in elastic.get_ids(indexprefix+'noises', scanId=scanid)
               if '{0}.{1}'.format(scanid, segment) in Id]
        if len(Ids):
            source = elastic.get_doc(indexprefix+'noises', Ids[0])['_source']
            zf = source['zerofrac']
            vnoise = source['noiseperbl']
            inoise = source['imstd']
        else:
            logger.warn("No noises found for {0}.{1}".format(scanid, segment))

        t0 = time.Time.now().mjd
        while time.Time.now().mjd-t0 < timeout/(24*3600):
            try:
                source = elastic.get_doc(indexprefix+'cands', candId)['_source'] 
            except NotFoundError:
                logger.warn("No candId {0} found".format(candId))
                break

            if 'frbprob' in source:
                frbprob = source['frbprob']
                if frbprob > 0.9:
                    label = "Good"
                elif frbprob > 0.1:
                    label = "Marginal"
                else:
                    label = "Questionable"
                break
            else:
                logger.info("No frbprob available for {0} yet...".format(candId))
                sleep(10)

        if time.Time.now().mjd-t0 >= timeout/(24*3600):
            logger.warn("Timed out querying for frbprob on {0}".format(candId))

    dd = {'primary_filesetId': cc.metadata.datasetId,
          'portal_candidate_IDs': candids,
          'transient_RA': ra.replace('h', ':').replace('m', ':').rstrip('s'),
          'transient_RA_error': float(pixel_sec)/15,  # seconds of time
          'transient_Dec': dec.replace('d', ':').replace('m', ':').rstrip('s'),
          'transient_Dec_error': float(pixel_sec),
          'transient_SNR': float(maxsnr),
          'transient_DM': float(cc.canddm[0]),
          'transient_DM_error': float(dmarr[1]-dmarr[0]),
          'preaverage_time': float(cc.canddt[0]),
          'rfpipe_version': cc.prefs.rfpipe_version,
          'prefs_Id': cc.prefs.name,
          'rf_QA_label': label,
          'rf_QA_zero_fraction': zf,
          'rf_QA_visibility_noise': vnoise,
          'rf_QA_image_noise': inoise}

    annotation = dd

    return annotation


def refine_candid(candid, indexprefix='new', ddm=50, npix_max=8192, npix_max_orig=None, mode='deployment', devicenum=None, cl=None):
    """ Given a candid, get SDM and refine it to make plot.
    """

    from rfpipe import reproduce
    from realfast import elastic

    doc = elastic.get_doc(indexprefix+'cands', Id=candid)
    if 'sdmname' not in doc['_source']:
        logger.warn("No SDM found for candId {0}".format(candid))
        return
    sdmname = doc['_source']['sdmname']
    prefsname = doc['_source']['prefsname']
    prefsdoc = elastic.get_doc(indexprefix+'preferences', Id=prefsname)
    if npix_max_orig is None:
        npix_max_orig = prefsdoc['_source']['npix_max']

    workdir = '/lustre/evla/test/realfast/archive/refined'
    sdmloc0 = '/home/mctest/evla/mcaf/workspace/'
    sdmloc1 = '/lustre/evla/test/realfast/archive/sdm_archive'
    sdmname_full = os.path.join(sdmloc0, sdmname) if os.path.exists(os.path.join(sdmloc0, sdmname)) else os.path.join(sdmloc1, sdmname)
    assert os.path.exists(sdmname_full)
    dm = doc['_source']['canddm']
    scanId = doc['_source']['scanId']
    refined_png = 'cands_{0}.1.1_refined.png'.format(sdmname)
    refined_loc = os.path.join(workdir, refined_png)
    refined_url = os.path.join(_candplot_url_prefix, 'refined', refined_png)

    def move_refined_plots(cc):
        if os.path.exists(refined_loc):
            logger.info("Refined candidate plot for candId {0} and sdm {1} found. Copying...".format(candid, sdmname))
            moveplots('/lustre/evla/test/realfast/archive/refined/', sdmname, destination='claw@nmpost-master:/lustre/aoc/projects/fasttransients/realfast/plots/refined')
        else:
            logger.info("No refinement plot found for candId {0}.".format(candid))

#        Ids = elastic.get_ids(indexprefix+'cands', sdmname)
#        if cc is not None:
        if os.path.exists(refined_loc):
            if len(cc):
                url = refined_url
                logger.info("Updating refinement plot for new new refined_url.")
            else:
                url = 'No candidate found during refinement'
                logger.info("Updating refinement plot for no refined_url.")
        else:
            url = 'No candidate found during refinement'
            logger.info("Updating refinement plot for no refined_url.")

#        for Id in Ids:
        elastic.update_field(indexprefix+'cands', 'refined_url', url, Id=candid)
        for k,v in elastic.gettags(indexprefix, candid).items():   # remove notify tag
            if 'notify' in v: 
                newtags = ','.join([tag for tag in v.split(',') if tag != 'notify'])
                elastic.update_field(indexprefix+'cands', k, newtags, Id=candid)

    # decide whether to submit or update index for known plots
    if os.path.exists(refined_loc):
        logger.info("Refined candidate plot for candId {0} and sdm {1} exists locally. Skipping.".format(candid, sdmname))
        return

    if cl is not None:
        logger.info("Submitting refinement for candId {0} and sdm {1}".format(candid, sdmname))
        workernames = [v['id'] for k, v in cl.scheduler_info()['workers'].items() if 'fetch' in v['id']]
        assert len(workernames)

        fut = cl.submit(reproduce.refine_sdm, sdmname_full, dm, preffile='/lustre/evla/test/realfast/realfast.yml',
                        npix_max=npix_max, npix_max_orig=npix_max_orig,
                        refine=True, classify=True, ddm=ddm, workdir=workdir,
                        resources={"GPU": 1}, devicenum=devicenum, retries=1, workers=workernames)

        fut2 = cl.submit(move_refined_plots, fut)
        distributed.fire_and_forget(fut2)
    else:
        logger.info("Running refinement for candId {0} and sdm {1}".format(candid, sdmname))
        cc = reproduce.refine_sdm(sdmname_full, dm, preffile='/lustre/evla/test/realfast/realfast.yml', npix_max_orig=npix_max_orig,
                                  npix_max=npix_max, refine=True, classify=True, ddm=ddm, workdir=workdir, devicenum=devicenum)
        move_refined_plots(cc)
            

def classify_candidates(cc, indexprefix='new', devicenum=None):
    """ Submit canddata object to node with fetch model ready
    """

    from rfpipe import candidates

    if isinstance(cc, distributed.Future):
        cc = cc.result()

    index = indexprefix + 'cands'

    if len(cc):
        assoc = find_bad(cc)  # find false positives

    try:
        if len(cc.canddata):
            logger.info("Running fetch classifier on {0} candidates for scanId {1}, "
                        "segment {2}"
                        .format(len(cc.canddata), cc.metadata.scanId, cc.segment))

            for i, cd in enumerate(cc.canddata):
                if assoc is not None:
                    if assoc[i]:
                        logger.info("Candidate {0} ({1}) failed find_bad. Skipping."
                                    .format(i, cd.candid))
                        continue
                frbprob = candidates.cd_to_fetch(cd, classify=True, devicenum=devicenum)
                elastic.update_field(index, 'frbprob', frbprob, Id=cd.candid)
        else:
            logger.info("No canddata available for scanId {0}, segment {1}."
                        .format(cc.metadata.scanId, cc.segment))
    except AttributeError:
        logger.info("CandCollection has no canddata attached. Not classifying.")


def buildsdm(sdmname, candid, indexprefix=None, copybdf=True):
    """ Build and SDM/BDF from the SDM and BDF.
    """

    import glob

    if sdmname is None:
        assert indexprefix is not None
        from realfast import elastic
        if candid is None:
            logger.exception("Need to provide canid or sdmname")
        doc = elastic.get_doc(indexprefix + 'cands', candid)
        assert 'sdmname' in doc['_source'], 'No sdmname associated with that candid'
        sdmname = doc['_source']['sdmname'].split('/')[-1]
        logger.info("Got sdmname {0} from {1}cands index".format(sdmname, indexprefix))

    sdmloc = '/home/mctest/evla/mcaf/workspace/'
    sdmname_full = os.path.join(sdmloc, sdmname)
    if os.path.exists(sdmname_full):
        shutil.copytree(sdmname_full, os.path.join('.', sdmname), ignore_dangling_symlinks=True, symlinks=True)
    else:
        logger.info("Trying realfast temp archive...")
        sdmloc = '/lustre/evla/test/realfast/archive/sdm_archive'
        sdmname_full = os.path.join(sdmloc, sdmname)
        if os.path.exists(sdmname_full):
            shutil.copytree(sdmname_full, os.path.join('.', sdmname), ignore_dangling_symlinks=True, symlinks=True)
        else:
            logger.warn("No SDM found")
            return

    bdfdestination = os.path.join('.', sdmname, 'ASDMBinary')
    if not os.path.exists(bdfdestination):
        os.mkdir(bdfdestination)

    bdft = sdmname.split('_')[-1]
    # remove suffix for sdms created multiple times
    if bdft[-2] is '.':
        bdft = bdft[:-2]
    bdfdir = '/lustre/evla/wcbe/data/realfast/'
    bdf0 = glob.glob('{0}/*{1}'.format(bdfdir, bdft))
    if len(bdf0) == 1:
        bdf0 = bdf0[0].split('/')[-1]
        newbdfpath = os.path.join(bdfdestination, bdf0)
        if copybdf and os.path.islink(newbdfpath):
            os.unlink(newbdfpath)

        if not os.path.exists(newbdfpath):
            if copybdf:
                shutil.copy(os.path.join(bdfdir, bdf0), newbdfpath)
            else:
                os.symlink(os.path.join(bdfdir, bdf0), newbdfpath)
    elif len(bdf0) == 0:
        logger.warn("No bdf found for {0}".format(sdmname))
    else:
        logger.warn("Could not find unique bdf for {0} in {1}. No bdf copied.".format(sdmname, bdf0))


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
        text=message, icon_emoji=":robot_face:")
    assert response["ok"]
    if response["message"]["text"] != message:
        logger.warn("Response from Slack API differs from message sent. "
                    "Maybe just broken into multiple updates: {0}".format(response))


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
    """ Function called to initialize python on workers
    """

    from rfpipe import search, state, metadata, candidates, reproduce, source, util
    t0 = time.Time.now().mjd
    search.set_wisdom(512)
    st = state.State(inmeta=metadata.mock_metadata(t0, t0+0.001, 27, 16, 16*32, 4, 5e4, datasource='sim'))
    return st


def rsync(original, new):
    """ Uses subprocess.call to rsync from 'filename' to 'new'
    If new is directory, copies original in.
    If new is new file, copies original to that name.
    """

    assert os.path.exists(original), 'Need original file!'
    res = subprocess.call(["rsync", "--timeout", "30", "-a", original.rstrip('/'), new.rstrip('/')])

    return int(res == 0)
