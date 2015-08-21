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
bdfdir = '/lustre/evla/wcbe/data/no_archive'
sdmArchdir = '/home/mchammer/evla/sdm/' #!!! THIS NEEDS TO BE SET BY A CENTRALIZED SETUP/CONFIG FILE. # dummy dir: /home/cbe-master/realfast/fake_archdir
bdfArchdir = '/lustre/evla/wcbe/data/archive/' #!!! THIS NEEDS TO BE SET BY A CENTRALIZED SETUP/CONFIG FILE.

@click.command()
@click.option('--qname', default='default', help='Name of queue to monitor')
@click.option('--triggered/--all', '-t', default=False, help='Triggered recording of scans or save all? (default: all)')
@click.option('--archive', '-a', is_flag=True, help='After search defines goodscans, set this to create new sdm and archive it.')
@click.option('--verbose', '-v', help='More verbose (e.g. debugging) output', is_flag=True)
@click.option('--test', '-t', help='Run test version of the code (e.g. will only print rather than actually archive)', is_flag=True)
def monitor(qname, triggered, archive, verbose, test):
    """ Blocking loop that prints the jobs currently being tracked in queue 'qname'.
    Can optionally be set to do triggered data recording (archiving).
    """

    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if test:
        logger.info('Running test version of the code. Will NOT actually archive but will print messages.')
    else:
        logger.info('***WARNING: Running the production version of the code. WILL do archiving if archiving parameter set!!!')

    logger.debug('Monitoring queue running in verbose/debug mode.')
    logger.info('Monitoring queue %s in %s recording mode...' % (qname, ['all', 'triggered'][triggered]))
    q = Queue(qname, connection=conn0)

    jobids0 = []
    while 1:
        jobids = conn.scan(cursor=0, count=trackercount)[1]

        if jobids0 != jobids:
            logger.info('Tracking %d jobs' % len(jobids))
            logger.debug('jobids: %s' % str(jobids))
            sys.stdout.flush()
            jobids0 = jobids

        # filter all jobids to those that are finished pipeline jobs
        jobs = [q.fetch_job(jobid) for jobid in jobids if q.fetch_job(jobid).is_finished and ('RT.pipeline' in q.fetch_job(jobid).func_name)]

        # iterate over list of tail jobs (one expected per scan)
        for job in jobs:
            d, segments = job.args
            logger.info('Job %s finished with filename %s, scan %s, segments %s' % (str(job.id), d['filename'], d['scan'], str(segments)))

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
                rtutils.plot_summary(d['workdir'], d['fileroot'], sc.keys())
            except:
                logger.info('Trouble merging scans and plotting for scans %s in file %s. Removing from tracking queue.' % (str(sc.keys()), d['fileroot']))
                removejob(job.id)
                continue

            # 4) if last scan of sdm, start end-of-sb processing
            if d['scan'] == sc.keys()[-1]:
                logger.info('This job processed last scan of %s.' % d['filename'])

                # 4-0) optionally could check that other scans are in finishedjobs. baseline assumption is that last scan finishes last.

                # 4-1) use timeout to check that BDFs are actually written (perhaps superfluous)
                if not all([sc[i]['bdfstr'] for i in sc.keys()]):   # bdfstr=None if file not written/found
                    logger.info('Not all bdf written yet for %s and scan %d. Waiting...' % (d['filename'], d['scan']))
                    now = time.time()
                    while 1:
                        if all([sc[i]['bdfstr'] for i in sc.keys()]):
                            logger.info('All BDF written for %s.' % d['filename'])
                            break
                        elif time.time() - now > timeout:
                            logger.info('Timeout while waiting for BDFs in %s.' % d['filename'])
                            break
                        else:
                            time.sleep(2)
                        
                # 4-2) if doing triggered recording, get scans to save. otherwise, save all scans.
                if triggered:
                    logger.debug('Triggering is on. Saving cal scans and those with candidates.')
                    goodscans = [s for s in sc.keys() if 'CALIB' in sc[s]['intent']]  # minimal set to save

                    # if merged cands available, identify scans to archive.
                    # ultimately, this could be much more clever than finding non-zero count scans.
                    if os.path.exists(os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl')):
                        goodscans += rtutils.count_candidates(os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl'))
                    goodscans = uniq_sort(goodscans) #uniq'd scan list in increasing order
                else:
                    logger.debug('Triggering is off. Saving all scans.')
                    goodscans = sc.keys()

                scanstring = ','.join(str(s) for s in goodscans)
                logger.info('Found the following scans to archive: %s' % scanstring)

                # 4-3) Edit SDM to remove no-cand scans. Perl script takes SDM work dir, and target directory to place edited SDM.
                if archive:
                    assert 'bunker' not in os.path.dirname(sc[goodscans[0]]['bdfstr']), '*** BDFSTR ERROR: No messing with bunker bdfs!'
                    logger.debug('Archiving is on.')
                    logger.debug('Archiving directory info:')
                    logger.debug('Workdir: %s' % d['workdir'])
                    logger.debug('SDMarch: %s' % sdmArchdir)
                    logger.debug('SDM:     %s' % d['filename'])
                    logger.debug('BDFarch: %s' % bdfArchdir)
                    logger.debug('BDFwork: %s' % os.path.dirname(sc[goodscans[0]]['bdfstr']))
                    
                    subprocess.call(['sdm_chop-n-serve.pl', d['filename'], d['workdir'], scanstring])   # would be nice to make this Python

                    # 4) copy new SDM and good BDFs to archive locations
                    
                    # Set up names of source/target SDM files
                    sdmORIG = d['filename'].rstrip('/')
                    sdmFROM = d['filename'].rstrip('/') + "_edited"
                    sdmTO   = os.path.join(sdmArchdir, os.path.basename(d['filename'].rstrip('/')))

                    # Archive edited SDM
                    if test:
                        logger.info('TEST MODE. Would archive SDM %s to %s' % ( sdmFROM, sdmTO ))
                        touch(sdmFROM + ".archived")
                    else:
                        logger.info('Archiving SDM %s to %s' % ( sdmFROM, sdmTO ))
                        rtutils.rsync( sdmFROM, sdmTO )

                    # Remove old SDM and old edited copy
                    if test:
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
                        if test:
                            logger.info('TEST MODE. Would hardlink %s to %s' % ( bdfFROM, bdfTO ))
                            touch( bdfFROM + ".archived" )
                        else:
                            logger.debug('Hardlinking %s to %s' % ( bdfFROM, bdfTO ))
                            os.link( bdfFROM, bdfTO )
 
                    # Now delete all the hardlinks in our BDF working directory for this SB.
                    for scan in sc.keys():
                        bdfREMOVE = sc[scan]['bdfstr'].rstrip('/')
                        if test:
                            logger.info('TEST MODE. Would remove BDF %s' % bdfREMOVE )
                            touch( bdfREMOVE + '.delete' )
                        else:
                            logger.debug('Removing BDF %s' % bdfREMOVE )
                            logger.info('***NOTE (%s): not deleting no_archive hardlinks yet' % bdfREMOVE)
                            #!!! os.remove( bdfREMOVE ) #!!! WHEN CASEY SAYS GO

                else:
                    logger.debug('Archiving is off.')                            
 
                # 6) organize cands/noise files?
            else:
                logger.info('Scan %d is not last scan of scanlist %s.' % (d['scan'], str(sc.keys())))

            # job is finished, so remove from db
            removejob(job.id)

        sys.stdout.flush()
        time.sleep(2)

def addjob(jobid):
    """ Adds jobid as key in db. Value = 0.
    """

    conn.set(jobid, 0)

def removejob(jobid):
    """ Removes jobid from db.
    """

    status = conn.delete(jobid)
    if status:
        logger.info('jobid %s removed' % jobid)
    else:
        logger.info('jobid %s not removed' % jobid)

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
def failed():
    """ Quick dump of trace for all failed jobs
    """

    q = Queue('failed', connection=conn0)
    logger.info('Failed queue: %s' % q.jobs)
    for i in range(len(q.jobs)):
        logger.info('Failure %d' % i)
        logger.info('%s' % q.jobs[i].exc_info)

@click.command()
def requeue():
    """ Take jobs from failed queue and add them to default queue
    """

    qf = Queue('failed', connection=conn0)
    logger.info('Enqueuing %d failed jobs' % len(qf.jobs))

    q = Queue('default', connection=conn0)
    for job in qf.jobs:
        logger.info('Moved job %s' % job.id)
        q.enqueue_job(job)
        qf.remove(job)

@click.command()
@click.argument('qname')
def empty(qname):
    """ Empty qname
    """

    q = Queue(qname, connection=conn0)
    logger.info('Emptying queue %s' % qname)
    for job in q.jobs:
        q.remove(job)
        logger.info('Removed %s\r' % job.id)

@click.command()
def reset():
    """ Reset queues (both dbs)
    """

    for qname in ['default', 'failed']:
        q = Queue(qname, connection=conn0)
        logger.info('Emptying queue %s' % qname)
        for job in q.jobs:
            q.remove(job)
            logger.info('Removed %s' % job.id)

    logger.info('Emptying tracking queue')
    jobids = conn.scan()[1]
    for jobid in jobids:
        removejob(jobid)
        logger.info('Removed %s' % jobid)
