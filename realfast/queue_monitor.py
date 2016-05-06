import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from redis import Redis
from rq.queue import Queue
from rq.registry import FinishedJobRegistry, StartedJobRegistry
import time, sys, os, glob
import subprocess, click, shutil
import sdmreader
from realfast import rtutils
import rtpipe.parsecands as pc

# set up  
conn0 = Redis(db=0)
conn = Redis(db=1)   # db for tracking ids of tail jobs
trackercount = 2000  # number of tracking jobs (one per scan in db=1) to monitor 
sdmwait = 3600    # timeout in seconds from last update of sdm to assume writing is finished
snrmin = 6.0
sdmArchdir = '/home/mchammer/evla/sdm/' #!!! THIS NEEDS TO BE SET BY A CENTRALIZED SETUP/CONFIG FILE. # dummy dir: /home/cbe-master/realfast/fake_archdir
bdfArchdir = '/lustre/evla/wcbe/data/archive/' #!!! THIS NEEDS TO BE SET BY A CENTRALIZED SETUP/CONFIG FILE.
redishost = os.uname()[1]  # assuming we start on redis host
scanind = 0  # available in state dict, but setting here since is stable and faster this way

@click.command()
@click.option('--qname', default='default', help='Name of queue to monitor')
@click.option('--triggered/--all', default=False, help='Triggered recording of scans or save all? (default: all)')
@click.option('--archive', '-a', is_flag=True, help='After search defines goodscans, set this to create new sdm and archive it.')
@click.option('--verbose', '-v', help='More verbose (e.g. debugging) output', is_flag=True)
@click.option('--nrao_controls_archiving', '-N', help='NRAO controls archiving; i.e. we make our own BDF hardlinks to no_archive and do no archiving at the end. If this is selected, mcaf_monitor MUST ALSO be run with this same option set.', is_flag=True)
@click.option('--production', help='Run code in full production mode (otherwise just runs as test)', is_flag=True)
@click.option('--threshold', help='Detection threshold used to trigger scan archiving (if --triggered set).', type=float, default=0.)
@click.option('--bdfdir', help='Directory to look for bdfs.', default='/lustre/evla/wcbe/data/no_archive')
def monitor(qname, triggered, archive, verbose, nrao_controls_archiving, production, threshold, bdfdir):
    """ Blocking loop that prints the jobs currently being tracked in queue 'qname'.
    Can optionally be set to do triggered data recording (archiving).
    """

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if nrao_controls_archiving:
        logger.info('***WARNING: NRAO WILL CONTROL ARCHIVING. Check to make sure that this option is also set in mcaf_monitor!!')
        assert not archive, '*** INPUT ERROR: "archive" and "nrao_controls_archiving" inputs are incompatible!'

    if production:
        logger.info('***WARNING: Running the production version of the code.***')
        if archive:
            logger.info('***WARNING: Will do archiving.***')
    else:
        logger.info('Running test version of the code. Will NOT actually archive but will print messages.')

    logger.debug('Monitoring queue running in verbose/debug mode.')
    logger.info('Monitoring queue %s in %s recording mode...' % (qname, ['all', 'triggered'][triggered]))
    q = Queue(qname, connection=conn0)
    qs = Queue('slow', connection=conn0)

    jobids0 = []
    q0hist = [0]
    q1hist = [0]
    sdmlastwritten = {}; sdmcount = {}
    while 1:
        jobids = conn.scan(cursor=0, count=trackercount)[1]

        # track history of queue sizes. saves/prints if values change.
        q0len = len(q.jobs)
        q1len = len(jobids)
        if (q0len != q0hist[-1]) or (q1len != q1hist[-1]):
            q0hist.append(q0len)  # track latest size
            q1hist.append(q1len)
            q0hist = q0hist[-10:]   # keep most recent 10
            q1hist = q1hist[-10:]
            logger.info('** Queue size history (newest to oldest) **')
            logger.info('Worker queue:\t%s' % q0hist[::-1])
            logger.info('Tail queue:\t%s' % q1hist[::-1])

        if jobids0 != jobids:
            logger.info('Tracking %d jobs' % len(jobids))
            logger.debug('jobids: %s' % str(jobids))
            sys.stdout.flush()
            jobids0 = jobids

        # filter all jobids to those that are finished pipeline jobs. now assumes only RT.pipeline jobs in queue
        badjobs = [jobids[i] for i in range(len(jobids)) if not q.fetch_job(jobids[i])]  # clean up jobids list first
        if badjobs:
            logger.info('Cleaning up jobs in tail queue with no counterpart in working queue.')
            for jobid in badjobs:
                rtutils.removejob(jobid)

        # update dict of sdm last written times whenever new scan appears for that filename
        filenames = [q.fetch_job(jobid).args[0]['filename'] for jobid in jobids]
        for filename in set(filenames):
            count = filenames.count(filename)
            if sdmcount.has_key(filename):
                if count > sdmcount[filename]:  # if new scan added for filename, then update time stamp
                    logger.info('Updated last written time for %s' % filename)
                    sdmlastwritten[filename] = time.time()
                sdmcount[filename] = count
            else:  # if no key exists, this one is new, so set initialize
                logger.info('Initialized last written time for %s' % filename)
                sdmlastwritten[filename] = time.time()
                sdmcount[filename] = count

        finishedjobs = [q.fetch_job(jobid) for jobid in jobids if q.fetch_job(jobid).is_finished] # and ('RT.pipeline' in q.fetch_job(jobid).func_name)]
        
        # iterate over list of finished tail jobs (one expected per scan)
        for job in finishedjobs:
            d, segments = job.args
            logger.info('Job %s finished with filename %s, scan %s, segments %s' % (str(job.id), d['filename'], d['scan'], str(segments)))
            readytoarchive = True # default assumption is that this file is ready to move to archive and clear from tracking queue

            jobids = conn.scan(cursor=0, count=trackercount)[1]  # refresh jobids to get latest scans_in_queue
            scans_in_queue = [q.fetch_job(jobid).args[0]['scan'] for jobid in jobids if q.fetch_job(jobid).args[0]['filename'] == d['filename']]
            logger.debug("Scans in queue for filename %s: %s" % (d['filename'], scans_in_queue))

            # To be done for each scan:

            # Error check directory usage/settings
            assert 'bunker' not in bdfdir, '*** BDFDIR ERROR: No messing with bunker bdfs!'
            assert 'telcal' not in bdfdir, '*** BDFDIR ERROR: No messing with telcal bdfs!'
            assert 'mchammer' not in d['workdir'] and 'mctest' not in d['workdir'], '*** WORKDIR ERROR: bunker, mchammer, and mctest are off-limits for writing!'
            assert 'mchammer' not in d['filename'] and 'mctest' not in d['filename'], '*** FILENAME ERROR: bunker, mchammer, and mctest are off-limits for writing!'

            # 0) may want to check that other segments finished for this scan. should do so by default ordering in queue

            # 1) merge segments. removes segment pkls, if successfully merged.
            try:
                pc.cleanup(d['workdir'], d['fileroot'], [d['scan']])
            except:
                logger.error('Could not cleanup cands/noise files for fileroot %s and scan %d. Removing from tracking queue.' % (d['fileroot'], d['scan']))
                rtutils.removejob(job.id)
                scans_in_queue.remove(d['scan'])
                continue

            # 2) get metadata (and check that file still available to work with)
            try:
                # Each sc key contains a dictionary. The key is the scan number.                            
                sc,sr = sdmreader.read_metadata(d['filename'], bdfdir=bdfdir)
            except:
                logger.error('Could not parse sdm %s. Removing from tracking queue.' % d['filename'])
                rtutils.removejob(job.id)
                scans_in_queue.remove(d['scan'])
                continue

            # 3) merge candidate and noise files from scans to merge
            # 4) move products into subdirectory "archivedir" and compile notebook
            try:
                if job == finishedjobs[-1]:  # only do summary plot if last in group to keep from getting bogged down with lots of cands

                    # create merge files and notebook products
                    pc.merge_scans(d['workdir'], d['fileroot'], sc.keys())
                    mergepkl = os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl')
                    noisepkl = os.path.join(d['workdir'], 'noise_' + d['fileroot'] + '_merge.pkl')

                    pc.nbcompile(d['workdir'], d['fileroot'])
                    notebook = os.path.join(d['workdir'], d['fileroot'] + '.ipynb')
                    notebookhtml = os.path.join(d['workdir'], d['fileroot'] + '.html')

                    # push off to archivedir
                    archivestr = '.'.join(d['fileroot'].split('.')[-2:])
                    if not archivestr.replace('.', '').isdigit():  # should have gotten decimal mjd
                        logger.warn('archivestr not parsed correctly ({0}). Will not move final products into subdirectory.'.format(archivestr))
                    else:
                        archivedir = os.path.join(d['workdir'], archivestr)
                        if not os.path.exists(archivedir):
                            os.mkdir(archivedir)
                            logger.info('Creating local archive directory at {0}'.format(archivedir))
                        else:
                            logger.info('Using local archive directory at {0}'.format(archivedir))

                        shutil.copy(mergepkl, archivedir)
                        shutil.copy(noisepkl, archivedir)
                        shutil.copy(notebook, archivedir)
                        shutil.copy(notebookhtml, archivedir)
                        logger.info('Copied merged products into local archive directory {0}'.format(archivedir))
                        # maybe copy GN file in too?
            except:
                logger.info('Trouble merging scans and plotting for scans %s in file %s. Removing from tracking queue.' % (str(sc.keys()), d['fileroot']))
                logger.debug('Exception:\n{0}'.format(sys.exc_info()[0]))
                rtutils.removejob(job.id)
                scans_in_queue.remove(d['scan'])
                continue

            # 5) rsync the interactive html and associated candidate plots out for inspection (note: cand plots may be delayed, so final rsync needed)
            try:
                rtutils.moveplots(d['fileroot'])
            except:
                logger.error('Failed to move cand plots and interactive plot out')

            # 6) if last scan of sdm, start end-of-sb processing. requires all bdf written or sdm not updated in sdmwait period
            allbdfwritten = all([sc[i]['bdfstr'] for i in sc.keys()])
            sdmtimeout = time.time() - sdmlastwritten[d['filename']] > sdmwait
            logger.debug('allbdfwritten = %s. sdmtimeout = %s.' % (str(allbdfwritten), str(sdmtimeout)))
            if (sdmtimeout or allbdfwritten) and (len(scans_in_queue) == 1) and (d['scan'] in scans_in_queue):
                logger.info('This job processed scan %d, the last scan in the queue for %s.' % (d['scan'], d['filename']))

                # 6-2) if doing triggered recording, get scans to save. otherwise, save all scans.
                if triggered:
                    logger.debug('Triggering is on. Saving cal scans and those with candidates.')
                    goodscans = [s for s in sc.keys() if 'CALIBRATE' in sc[s]['intent']]  # minimal set to save

                    # if merged cands available, identify scans to archive.
                    # ultimately, this could be much more clever than finding non-zero count scans.
                    if os.path.exists(mergepkl):
                        goodscans += [sigloc[scanind] for sigloc in pc.thresholdcands(mergepkl, threshold, numberperscan=1)]
                    goodscans = sorted(set(goodscans))  # uniq'd scan list in increasing order
                else:
                    logger.debug('Triggering is off. Saving all scans.')
                    goodscans = sc.keys()

                goodscanstr= ','.join(str(s) for s in goodscans)
                logger.info('Found the following scans to archive: %s' % goodscanstr)

# SLOW QUEUE ON THE SHELF FOR NOW...
#                # 6-3) Edit SDM to remove no-cand scans. Perl script takes SDM work dir, and target directory to place edited SDM.
#                # first determine if this filename is still being worked on by slow queue
#                slowjobids = qs.job_ids # + getstartedjobs('slow')  # working and queued for slow queue
#                remaining = [jobid for jobid in slowjobids if os.path.basename(d['filename']).rstrip('.pkl') in qs.fetch_job(jobid).args[0]]  # these jobs are still open for this file
#
#                if len(remaining) == 0:
#                    logger.info('No jobs for file %s in slow queue. Moving candidate scan data to archive.' % (d['filename']))
#                else:  # slow queue needs more time
#                    logger.info('File %s is still being worked on in slow queue. Will not move to archive yet.' % d['filename'])
#                    logger.debug('remaining jobids: %s' % str(remaining))
#                    readytoarchive = False  # looks like we're not ready! use this below to keep file in tracking queue
#                    continue

                # Once slow queue is finished, either 
                if archive:
                    movetoarchive(d['filename'], d['workdir'].rstrip('/'), goodscanstr, production, bdfdir)
                elif nrao_controls_archiving:
                    logger.debug('Will remove BDFs')
                    removebdfs(d['filename'], d['workdir'].rstrip('/'), production, bdfdir)
                else:
                    logger.debug('Archiving is off.')

                # 6-4) Combine MS files from slow integration into single file. Merges only MS files it finds from provided scan list.
#                try:
#                    rtutils.mergems(d['filename'], sc.keys(), redishost=redishost)
#                except:
#                    logger.info('Failed to merge slow MS files. Continuing...')
 
                # final rsync to get html and cand plots out for inspection
                try:
                    rtutils.moveplots(d['fileroot'])
                except:
                    logger.error('Failed to move cand plots and interactive plot out')

            elif not allbdfwritten and (len(scans_in_queue) == 1) and (d['scan'] in scans_in_queue):
                logger.info('Not all bdf written yet. Keeping last scan of %s in tracking queue.' % d['filename'])
                readytoarchive = False  # looks like we're not ready! use this below to keep file in tracking queue

            else:
                logger.info('Scan %d is not last scan or %s is not finished writing.' % (d['scan'], d['filename']))
                logger.debug('List of bdfstr: %s. scans_in_queue = %s.' % (str([sc[i]['bdfstr'] for i in sc.keys()]), str(scans_in_queue)))

            # job is finished, so remove from db
            if readytoarchive:  # will be false is slow queue not yet empty of relevant jobs
                logger.info('Removing job %s from tracking queue.' % job.id)
                rtutils.removejob(job.id)
                scans_in_queue.remove(d['scan'])
                sys.stdout.flush()
            else:
                logger.info('Keeping job %s from tracking queue.' % job.id)

        sys.stdout.flush()
        time.sleep(1)

def removebdfs(filename, workdir, production, bdfdir):
    """ Moves sdm and bdf associated with filename to archive.
    filename is sdmfile. workdir is place with file.
    goodscanstr is comma-delimited list, which is optional.
    production is boolean for production mode.
    """

    assert filename, 'Need filename to move to archive'

    if not workdir:
        workdir = os.getcwd()
    sc,sr = sdmreader.read_metadata(filename, bdfdir=bdfdir)


    # Clean up all the hardlinks in no_archive for this SB.
    for scan in sc.keys():
        # System safety checks
        assert 'bunker' not in os.path.dirname(sc[scan]['bdfstr']), '*** BDFSTR ERROR: No messing with bunker bdfs!'
        assert 'telcal' not in os.path.dirname(sc[scan]['bdfstr']), '*** BDFSTR ERROR: No messing with telcal bdfs!'
        bdfREMOVE = sc[scan]['bdfstr']
        if bdfREMOVE:
            if not production:
                logger.info('TEST MODE. Would remove BDF %s' % bdfREMOVE.rstrip('/') )
                touch( bdfREMOVE.rstrip('/') + '.delete' )
            else:
                logger.info('Removing BDF %s' % bdfREMOVE.rstrip('/') )
                os.remove( bdfREMOVE.rstrip('/') )


def movetoarchive(filename, workdir, goodscanstr, production, bdfdir):
    """ Moves sdm and bdf associated with filename to archive.
    filename is sdmfile. workdir is place with file.
    goodscanstr is comma-delimited list, which is optional.
    production is boolean for production mode.
    """

    assert filename, 'Need filename to move to archive'

    if not workdir:
        workdir = os.getcwd()
    sc,sr = sdmreader.read_metadata(filename, bdfdir=bdfdir)
    if not goodscanstr:
        goodscanstr = ','.join([str(s) for s in sc.keys() if sc[s]['bdfstr']])
    goodscans = [int(s) for s in goodscanstr.split(',')]

    logger.debug('Archiving is on.')
    logger.debug('Archiving directory info:')
    logger.debug('Workdir: %s' % workdir)    # !! could just be set to os.getcwd()?
    logger.debug('SDMarch: %s' % sdmArchdir)
    logger.debug('SDM:     %s' % filename)
    logger.debug('BDFarch: %s' % bdfArchdir)
    logger.debug('BDFwork: %s' % bdfdir)

    # Set up names of source/target SDM files
    sdmORIG = filename.rstrip('/')
    sdmFROM = filename.rstrip('/') + "_edited"
    sdmTO   = os.path.join(sdmArchdir, os.path.basename(filename.rstrip('/')))

    # safety checks
    if not goodscans:
        raise ValueError, 'No scans found to move to archive (either none provided or none with bdfs).'
    assert 'bunker' not in os.path.dirname(sc[goodscans[0]]['bdfstr']), '*** BDFSTR ERROR: No messing with bunker bdfs!'
    assert 'telcal' not in os.path.dirname(sc[goodscans[0]]['bdfstr']), '*** BDFSTR ERROR: No messing with telcal bdfs!'
    assert not os.path.exists(sdmFROM), 'Edited SDM already exists at %s' % sdmFROM

    if production:
        subprocess.call(['sdm_chop-n-serve.pl', filename, workdir, goodscanstr])   # would be nice to make this Python

    # 4) copy new SDM and good BDFs to archive locations
                    
    # Archive edited SDM
    if not production:
        logger.info('TEST MODE. Would archive SDM %s to %s' % ( sdmFROM, sdmTO ))
        touch(sdmFROM + ".archived")
    else:
        logger.info('Archiving SDM %s to %s' % ( sdmFROM, sdmTO ))
        rtutils.rsync( sdmFROM, sdmTO )

    # Remove old SDM and old edited copy
    if not production:
        logger.info('TEST MODE. Would delete edited SDM %s' % sdmFROM )
        logger.info('TEST MODE. Would delete original SDM %s' % sdmORIG )
        touch(sdmFROM + ".delete")
        touch(sdmORIG + ".delete")
    else: 
        logger.debug('Deleting edited SDM %s' % sdmFROM )
        shutil.rmtree( sdmFROM )
        logger.info('***NOTE (%s): not deleting unedited SDM files yet' % sdmORIG )
        #!!!logger.debug('Deleting original SDM %s' % sdmORIG ) #!!! WHEN CASEY SAYS GO
        #!!!shutil.rmtree( sdmORIG ) #!!! PUT THIS LINE IN WHEN CASEY SAYS GO


    # Archive the BDF (via hardlink to archdir) only if we are controlling the archiving.
    for scan in goodscans:
        bdfFROM = sc[scan]['bdfstr']
        bdfTO   = os.path.join(bdfArchdir, os.path.basename(bdfFROM))
        if not production:
            logger.info('TEST MODE. Would hardlink %s to %s' % ( bdfFROM, bdfTO ))
            touch( bdfFROM + ".archived" )
        else:
            logger.debug('Hardlinking %s to %s' % ( bdfFROM, bdfTO ))
            os.link( bdfFROM, bdfTO )

    # Now delete all the hardlinks in our BDF working directory for this SB.
    # Could also use removebdfs() here.
    for scan in sc.keys():
        bdfREMOVE = sc[scan]['bdfstr']
        if bdfREMOVE:
            if not production:
                logger.info('TEST MODE. Would remove BDF %s' % bdfREMOVE.rstrip('/') )
                touch( bdfREMOVE.rstrip('/') + '.delete' )
            else:
                logger.debug('Removing BDF %s' % bdfREMOVE.rstrip('/') )
                os.remove( bdfREMOVE.rstrip('/') )

def getfinishedjobs(qname='default'):
    """ Get list of job ids in finished registry.
    """

    q = Queue(qname, connection=conn0)
    return FinishedJobRegistry(name=q.name, connection=conn0).get_job_ids()

def getstartedjobs(qname='default'):
    """ Get list of job ids in started registry.
    """

    q = Queue(qname, connection=conn0)
    return StartedJobRegistry(name=q.name, connection=conn0).get_job_ids()

# Temporary method for creating an empty file.
def touch(path):
    with open(path, 'a'):
        os.utime(path, None)

