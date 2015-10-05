import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from redis import Redis
from rq.queue import Queue
from rq.registry import FinishedJobRegistry
import time, sys, os
import subprocess, click, shutil
import sdmreader
from realfast import rtutils

# set up  
conn0 = Redis(db=0)
conn = Redis(db=1)   # db for tracking ids of tail jobs
timeout = 600   # seconds to wait for BDF to finish writing (after final pipeline job completes)
trackercount = 2000  # number of tracking jobs (one per scan in db=1) to monitor 
snrmin = 6.0
inttime = '5'  # time to integrate when making slow copy
bdfdir = '/lustre/evla/wcbe/data/no_archive'
sdmArchdir = '/home/mchammer/evla/sdm/' #!!! THIS NEEDS TO BE SET BY A CENTRALIZED SETUP/CONFIG FILE. # dummy dir: /home/cbe-master/realfast/fake_archdir
bdfArchdir = '/lustre/evla/wcbe/data/archive/' #!!! THIS NEEDS TO BE SET BY A CENTRALIZED SETUP/CONFIG FILE.
redishost = os.uname()[1]  # assuming we start on redis host

@click.command()
@click.option('--qname', default='default', help='Name of queue to monitor')
@click.option('--triggered/--all', default=False, help='Triggered recording of scans or save all? (default: all)')
@click.option('--archive', '-a', is_flag=True, help='After search defines goodscans, set this to create new sdm and archive it.')
@click.option('--verbose', '-v', help='More verbose (e.g. debugging) output', is_flag=True)
@click.option('--production', help='Run code in full production mode (otherwise just runs as test)', is_flag=True)
@click.option('--threshold', help='Detection threshold used to trigger scan archiving (if --triggered set).', type=float, default=0.)
def monitor(qname, triggered, archive, verbose, production, threshold):
    """ Blocking loop that prints the jobs currently being tracked in queue 'qname'.
    Can optionally be set to do triggered data recording (archiving).
    """

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if production:
        logger.info('***WARNING: Running the production version of the code.***')
        if archive:
            logger.info('***WARNING: Will do archiving.***')
    else:
        logger.info('Running test version of the code. Will NOT actually archive but will print messages.')

    logger.debug('Monitoring queue running in verbose/debug mode.')
    logger.info('Monitoring queue %s in %s recording mode...' % (qname, ['all', 'triggered'][triggered]))
    q = Queue(qname, connection=conn0)

    jobids0 = []
    q0hist = [0]
    q1hist = [0]
    while 1:
        jobids = conn.scan(cursor=0, count=trackercount)[1]

        # track history of queue sizes
        # if either queue changes size, save value
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

        # filter all jobids to those that are finished pipeline jobs. now assumes only RT.pipeline jobs in q
        badjobs = [jobids[i] for i in range(len(jobids)) if not q.fetch_job(jobids[i])]  # clean up jobids list first
        if badjobs:
            logger.info('Cleaning up jobs in tail queue with no counterpart in working queue.')
            for jobid in badjobs:
                removejob(jobid)

        jobs = [q.fetch_job(jobid) for jobid in jobids if q.fetch_job(jobid).is_finished] # and ('RT.pipeline' in q.fetch_job(jobid).func_name)]
        
        # iterate over list of tail jobs (one expected per scan)
        for job in jobs:
            d, segments = job.args
            logger.info('Job %s finished with filename %s, scan %s, segments %s' % (str(job.id), d['filename'], d['scan'], str(segments)))

            scans_in_queue = [q.fetch_job(jobid).args[0]['scan'] for jobid in jobids if q.fetch_job(jobid).args[0]['filename'] == d['filename']]
            logger.debug("Scans in queue for filename %s: %s" % (d['filename'], scans_in_queue))

            # To be done for each scan:

            # Error check directory usage/settings
            assert 'bunker' not in bdfdir, '*** BDFDIR ERROR: No messing with bunker bdfs!'
            assert 'mchammer' not in d['workdir'] and 'mctest' not in d['workdir'], '*** WORKDIR ERROR: bunker, mchammer, and mctest are off-limits for writing!'
            assert 'mchammer' not in d['filename'] and 'mctest' not in d['filename'], '*** FILENAME ERROR: bunker, mchammer, and mctest are off-limits for writing!'

            # 0) may want to check that other segments finished for this scan. should do so by default ordering in queue

            # 1) merge segments. removes segment pkls, if successfully merged.
            try:
                rtutils.cleanup(d['workdir'], d['fileroot'], [d['scan']])
            except:
                logger.error('Could not cleanup cands/noise files for fileroot %s and scan %d. Removing from tracking queue.' % (d['fileroot'], d['scan']))
                removejob(job.id)
                continue

            # 2) get metadata (and check that file still available to work with)
            try:
                # Each sc key contains a dictionary. The key is the scan number.                            
                sc,sr = sdmreader.read_metadata(d['filename'], bdfdir=bdfdir)
            except:
                logger.error('Could not parse sdm %s. Removing from tracking queue.' % d['filename'])
                removejob(job.id)
                continue

            # 3) aggregate cands/noise files and plot available so far. creates/overwrites the merge pkl
            try:
                rtutils.plot_summary(d['workdir'], d['fileroot'], sc.keys(), snrmin=snrmin)
            except:
                logger.info('Trouble merging scans and plotting for scans %s in file %s. Removing from tracking queue.' % (str(sc.keys()), d['fileroot']))
                removejob(job.id)
                continue

            # 4) if last scan of sdm, start end-of-sb processing
            if all([sc[i]['bdfstr'] for i in sc.keys()]) and (len(scans_in_queue) == 1) and (d['scan'] in scans_in_queue):
                logger.info('This job processed scan %d, the last scan in the queue for %s.' % (d['scan'], d['filename']))

                # 4-0) enqueue job to integrate SDM down into MS
                allscanstr = ','.join(str(s) for s in sc.keys())
                rtutils.integrate(d['filename'], allscanstr, inttime, redishost)

                # 4-1) if doing triggered recording, get scans to save. otherwise, save all scans.
                if triggered:
                    logger.debug('Triggering is on. Saving cal scans and those with candidates.')
                    goodscans = [s for s in sc.keys() if 'CALIB' in sc[s]['intent']]  # minimal set to save

                    # if merged cands available, identify scans to archive.
                    # ultimately, this could be much more clever than finding non-zero count scans.
                    if os.path.exists(os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl')):
                        goodscans += rtutils.find_archivescans(os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl'), threshold)
                        #!!! For rate tests: print cand info !!!
                        rtutils.tell_candidates(os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl'), os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.snrlist'))
                    goodscans = uniq_sort(goodscans) #uniq'd scan list in increasing order
                else:
                    logger.debug('Triggering is off. Saving all scans.')
                    goodscans = sc.keys()

                logger.info('Found the following scans to archive: %s' % ','.join(str(s) for s in goodscans))

                # 4-2) Edit SDM to remove no-cand scans. Perl script takes SDM work dir, and target directory to place edited SDM.
                if archive:
                    movetoarchive(d['filename'], d['workdir'].rstrip('/'), goodscans=goodscans, production=production)
                else:
                    logger.debug('Archiving is off.')                            
 
                # 6) organize cands/noise files?
            else:
                logger.info('Scan %d is not last scan or %s is not finished writing.' % (d['scan'], d['filename']))
                logger.debug('List of bdfstr: %s. scans_in_queue = %s.' % (str([sc[i]['bdfstr'] for i in sc.keys()]), str(scans_in_queue)))

            # job is finished, so remove from db
            logger.info('Removing job %s from tracking queue.' % job.id)
            removejob(job.id)
            sys.stdout.flush()

        sys.stdout.flush()
        time.sleep(1)


@click.command()
@click.argument('filename')
@click.argument('workdir')
@click.option('--goodscans', help='List of scans to archive. Default is to archive all.')
@click.option('--production', help='Run code in full production mode (otherwise just runs as test)', is_flag=True)
def movetoarchive(filename, workdir, goodscans=None, production=False):
    """ Moves sdm and bdf associated with filename to archive.
    filename is sdmfile. workdir is place with file.
    goodscans is list, which is optional.
    production is boolean for production mode.
    """

    sc,sr = sdmreader.read_metadata(filename, bdfdir=bdfdir)
    if not goodscans:
        goodscans = [s for s in sc.keys() if sc[s]['bdfstr']]

    logger.debug('Archiving is on.')
    logger.debug('Archiving directory info:')
    logger.debug('Workdir: %s' % workdir)    # !! could just be set to os.getcwd()?
    logger.debug('SDMarch: %s' % sdmArchdir)
    logger.debug('SDM:     %s' % filename)
    logger.debug('BDFarch: %s' % bdfArchdir)
    logger.debug('BDFwork: %s' % os.path.dirname(sc[goodscans[0]]['bdfstr']))
    assert 'bunker' not in os.path.dirname(sc[goodscans[0]]['bdfstr']), '*** BDFSTR ERROR: No messing with bunker bdfs!'

    scanstring = ','.join(str(s) for s in goodscans)
    subprocess.call(['sdm_chop-n-serve.pl', filename, workdir, scanstring])   # would be nice to make this Python

    # 4) copy new SDM and good BDFs to archive locations
                    
    # Set up names of source/target SDM files
    sdmORIG = filename.rstrip('/')
    sdmFROM = filename.rstrip('/') + "_edited"
    sdmTO   = os.path.join(sdmArchdir, os.path.basename(filename.rstrip('/')))

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

    # Archive the BDF (via hardlink to archdir)
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
    for scan in goodscans:
        bdfREMOVE = sc[scan]['bdfstr'].rstrip('/')
        if not production:
            logger.info('TEST MODE. Would remove BDF %s' % bdfREMOVE )
            touch( bdfREMOVE + '.delete' )
        else:
            logger.debug('Removing BDF %s' % bdfREMOVE )
            logger.info('***NOTE (%s): not deleting no_archive hardlinks yet' % bdfREMOVE)
            #!!! os.remove( bdfREMOVE ) #!!! WHEN CASEY SAYS GO

def addjob(jobid):
    """ Adds jobid as key in db. Value = 0.
    """

    conn.set(jobid, 0)

def removejob(jobid):
    """ Removes jobid from db.
    """

    status = conn.delete(jobid)
    if status:
        logger.info('jobid %s removed from tracking queue' % jobid)
    else:
        logger.info('jobid %s not removed from tracking queue' % jobid)

def getfinishedjobs(qname='default'):
    """ Get list of job ids in finished registry.
    """

    q = Queue(qname, connection=conn0)
    return FinishedJobRegistry(name=q.name, connection=conn0).get_job_ids()

# Temporary method for creating an empty file.
def touch(path):
    with open(path, 'a'):
        os.utime(path, None)

# Remove duplicates in a list (NOT order-preserving!)
def uniq_sort(lst):
    theset = set(lst)
    thelist = list(theset)
    thelist.sort()
    return thelist

@click.command()
def status():
    """ Quick dump of trace for all failed jobs
    """

    for qname in ['default', 'slow', 'failed']:
        q = Queue(qname, connection=conn0)
        logger.info('Jobs in queue %s:' % qname)
        for job in q.jobs:
            if isinstance(job.args[0], dict):
                details = (job.args[0]['filename'], job.args[0]['scan'])
            elif isinstance(job.args[0], str):
                details = job.args[0]
                logger.info('job %s: %s, segments, %s' % (job.id, str(details), str(job.args[1])))

    jobids = conn.scan(cursor=0, count=trackercount)[1]
    logger.info('Jobs in tracking queue:')
    q = Queue('default', connection=conn0)
    for jobid in jobids:
        job = q.fetch_job(jobid)
        logger.info('job %s: filename %s, scan %d, segments, %s' % (job.id, job.args[0]['filename'], job.args[0]['scan'], str(job.args[1])))

@click.command()
def failed():
    """ Quick dump of trace for all failed jobs
    """

    q = Queue('failed', connection=conn0)
    for i in range(len(q.jobs)):
        job = q.jobs[i]
        logger.info('Failed job %s, filename %s, scan %d, segments, %s' % (job.id, job.args[0]['filename'], job.args[0]['scan'], str(job.args[1])))
        logger.info('%s' % job.exc_info)

@click.command()
def requeue():
    """ Take jobs from failed queue and add them to default queue
    """

    qf = Queue('failed', connection=conn0)
    q = Queue('default', connection=conn0)
#    qs = Queue('slow', connection=conn0)  # how to requeue to slow also?
    for job in qf.jobs:
        logger.info('Requeuing job %s: filename %s, scan %d, segments, %s' % (job.id, job.args[0]['filename'], job.args[0]['scan'], str(job.args[1])))
        q.enqueue_job(job)
        qf.remove(job)

@click.command()
def clean():
    """ Take jobs from tracking queue and clean them from all queues if they or their dependencies have failed
    """

    q = Queue('default', connection=conn0)
    qf = Queue('failed', connection=conn0)
    jobids = conn.scan(cursor=0, count=trackercount)[1]
    for jobid in jobids:
        job = qf.fetch_job(jobid)
        jobd = qf.fetch_job(jobid).dependency
        if job.is_failed or jobd.is_failed:
            logger.info('Job(s) upstream of %s failed. Removing all dependent jobs from all queues.' % jobid)
            removejob(jobid)
            if job.is_failed:
                logger.info('cleaning up job %s: filename %s, scan %d, segments, %s' % (job.id, job.args[0]['filename'], job.args[0]['scan'], str(job.args[1])))
                q.remove(job)
                qf.remove(job)
            if jobd.is_failed:
                logger.info('cleaning up job %s: filename %s, scan %d, segments, %s' % (jobd.id, jobd.args[0]['filename'], jobd.args[0]['scan'], str(jobd.args[1])))
                q.remove(jobd)
                qf.remove(jobd)

@click.command()
@click.argument('qname')
def empty(qname):
    """ Empty qname
    """

    q = Queue(qname, connection=conn0)
    logger.info('Emptying queue %s' % qname)
    for job in q.jobs:
        if isinstance(job.args[0], dict):
            details = (job.args[0]['filename'], job.args[0]['scan'])
        elif isinstance(job.args[0], str):
            details = job.args[0]
        logger.info('Removed job %s: %s, segments, %s' % (job.id, str(details), str(job.args[1])))
        q.remove(job)

@click.command()
def reset():
    """ Reset queues (both dbs)
    """

    for qname in ['default', 'slow', 'failed']:
        q = Queue(qname, connection=conn0)
        logger.info('Emptying queue %s' % qname)
        for job in q.jobs:
            q.remove(job)
            logger.info('Removed job %s' % job.id)

    logger.info('Emptying tracking queue')
    jobids = conn.scan(cursor=0, count=trackercount)[1]
    for jobid in jobids:
        removejob(jobid)
