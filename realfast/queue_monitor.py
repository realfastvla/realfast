from redis import Redis
from rq.queue import Queue
from rq.registry import FinishedJobRegistry
import time, pickle, sys, logging, os
import sdmreader
import click
import shutil
from realfast import rtutils

conn0 = Redis(db=0)
conn = Redis(db=1)   # db for tracking ids of tail jobs
timeout = 600   # seconds to wait for BDF to finish writing (after final pipeline job completes)
logging.basicConfig(format="%(asctime)-15s %(levelname)8s %(message)s", level=logging.INFO)

@click.command()
@click.option('--qname', default='default', help='Name of queue to monitor')
@click.option('--triggered/--all', '-t', default=False, help='Triggered recording of scans or save all? (default: all)')
@click.option('--archive', '-a', is_flag=True, help='After search defines goodscans, set this to create new sdm and archive it.')
def monitor(qname, triggered, archive):
    """ Blocking loop that prints the jobs currently being tracked in queue 'qname'.
    Can optionally be set to do triggered data recording (archiving).
    """

    logging.info('Monitoring queue %s in %s recording mode...' % (qname, ['all', 'triggered'][triggered]))
    q = Queue(qname, connection=conn0)

    jobids0 = []
    while 1:
        jobids = conn.scan()[1]

        if jobids0 != jobids:
            logging.info('Tracking jobs: %s' % str(jobids))
            sys.stdout.flush()
            jobids0 = jobids

        for jobid in jobids:
            job = q.fetch_job(jobid)

            # if job is finished, check whether it is final scan of this sdm
            if job.is_finished:
                logging.info('Job %s finished.' % str(jobid))
                #!!! todo: check that all other segmentss are also finished? baseline assumption is that all segments finish before this one.
#                finishedjobs = getfinishedjobs(qname)

                # is this the last scan of sdm?
                if 'RT.pipeline' in job.func_name:
                    logging.debug('Finished job is RT.pipeline job.')
                    d, segments = job.args
                    sc,sr = sdmreader.read_metadata(d['filename'])
                    if d['scan'] == sc.keys()[-1]:
                        logging.info('This job processed last scan of %s.' % d['filename'])
                        #!!! todo: check that other scans are in finishedjobs. baseline assumption is that last scan finishes last

                        # check that BDFs are actually written (perhaps superfluous)
                        now = time.time()
                        logging.info('Waiting for all BDF to be written for %s.' % d['filename'])
                        while 1:
                            if all([sc[i]['bdfstr'] for i in sc.keys()]):
                                logging.info('All BDF written for %s.' % d['filename'])
                                break
                            elif time.time() - now > timeout:
                                logging.info('Timeout while waiting for BDFs in %s.' % d['filename'])
                                break
                            else:
                                time.sleep(1)
                        
                        # do "end of SB" processing
                        # 1) aggregate cands/noise files
                        rtutils.cleanup(d['workdir'], d['fileroot'], sc.keys())

                        # 2) if triggered recording, get scans with detections, else save all.
                        if triggered:  
                            goodscans = count_candidates(os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl'))
                            goodscans = goodscans + [s for s in sc.keys() if 'CALIB' in sc[s]['intent']]
                        else:
                            goodscans = sc.keys()

                        scanstring = ','.join(str(s) for s in goodscans)
                        logging.info('Found good scans: %s' % scanstring)

                        # 3) Edit SDM to remove no-cand scans. Perl script takes SDM work dir, and target directory to place edited SDM.
                        if archive:
                            sdmArchdir = '/home/cbe-master/realfast/fake_archdir' #'/home/mctest/evla/sdm/' #!!! THIS NEEDS TO BE SET BY A CENTRALIZED SETUP/CONFIG FILE.
                            subprocess.call(['sdm_chop-n-serve.pl', d['filename'], d['workdir'], scanstring])   # would be nice to make this Python

                            # 4) copy new SDM and good BDFs to archive locations
                            copyDirectory(d['filename'].rstrip('/') + "_edited", os.path.join(sdmArchdir, d['filename']))

                            #!!! FOR PRE-RUN TESTING: Need to fix these lines here to clean up: remove SDM and edited SDM
                            touch(d['filename'].rstrip('/') + "_edited.delete")
                            touch(d['filename'].rstrip('/') + ".delete")

                            # Each sc key contains a dictionary. The key is the scan number.                            
                            # Archive the BDF (via hardlink to archdir)
                            for scan in goodscans:
                                #!!! FOR PRE-RUN TESTING: write a .save to our realfast home workdir

                                touch(os.path.join(sdmArchdir, os.path.basename(sc[i]['bdfstr'])) + '.archive'))
                                #!!! PERMA-SOLUTION: hardlink the file
                                #!!!os.link(sc[i]['bdfstr'], os.path.join(bdfArchdir, os.path.basename(sc[i]['bdfstr'])) + '.archive'))

                            # Now delete all the hardlinks in our BDF working directory for this SB.
                            for scan in sc.keys():
                                touch(os.path.join(sdmArchdir, os.path.basename(sc[i]['bdfstr']) + '.delete'))
                            
 
                        # 5) finally plot candidates
                        rtutils.plot_summary(d['workdir'], d['fileroot'], sc.keys())

                        # 6) organize cands/noise files?

                    else:
                        logging.info('Scan %d is not last scan of scanlist %s.' % (d['scan'], str(sc.keys())))
                else:
                    logging.info('This is some other job: %s' % job.func_name)

                # job is finished, so remove from db
                removejob(jobid)

        # timeout tests?
        sys.stdout.flush()
        time.sleep(2)

def addjob(jobid):
    """ Adds jobid as key in db. Value = 0.
    """

    conn.set(jobid, 0)

def removejob(jobid):
    """ Removes jobid from db.
    """

    conn.delete(jobid)

def getfinishedjobs(qname='default'):
    """ Get list of job ids in finished registry.
    """

    q = Queue(qname, connection=conn0)
    return FinishedJobRegistry(name=q.name, connection=conn0).get_job_ids()

def count_candidates(mergefile):
    """ Parses merged cands file and returns dict of (scan, candcount).
    """
    with open(candsfile, 'rb') as pkl:
        d = pickle.load(pkl)
        cands = pickle.load(pkl)

    return list(set([kk[0] for kk in cands.keys()]))    

def copyDirectory(src, dest):
    try:
        shutil.copytree(src, dest)
    # Directories are the same
    except shutil.Error as e:
        print('Directory not copied. Error: %s' % e)
    # Any error saying that the directory doesn't exist
    except OSError as e:
        print('Directory not copied. Error: %s' % e)

# Temporary method for creating an empty file.
def touch(path):
    with open(path, 'a'):
        os.utime(path, None)

def failed():
    """ Quick dump of trace for all failed jobs
    """

    q = Queue('failed', connection=conn0)
    logging.info('Failed queue: %s' % q.jobs)
    for i in range(len(q.jobs)):
        logging.info('Failure %d' % i)
        logging.info('%s' % q.jobs[i].exc_info)
