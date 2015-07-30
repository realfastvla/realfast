from redis import Redis
from rq.queue import Queue
from rq.registry import FinishedJobRegistry
import time, pickle, sys
import sdmreader
import click
import numpy as n
import shutil

conn0 = Redis(db=0)
conn = Redis(db=1)   # db for tracking ids of tail jobs
timeout = 600   # seconds to wait for BDF to finish writing (after final pipeline job completes)

@click.command()
@click.option('--qname', default='default', help='Name of queue to monitor')
def monitor(qname):
    """ Blocking loop that prints the jobs currently being tracked.
    """

    print 'Monitoring queue %s...' % qname
    q = Queue(qname, connection=conn0)

    jobids0 = []
    while 1:
        jobids = conn.scan()[1]

        if jobids0 != jobids:
            print 'Tracking jobs: %s' % str(jobids)

        for jobid in jobids:
            job = q.fetch_job(jobid)

            # if job is finished, check whether it is final scan of this sdm
            if job.is_finished:
                print 'Job %s finished.' % str(jobid)
                #!!! todo: check that all other segmentss are also finished? baseline assumption is that all segments finish before this one.
#                finishedjobs = getfinishedjobs(qname)

                # is this the last scan of sdm?
                if 'RT.pipeline' in job.func_name:
                    d, segments = job.args
                    sc,sr = sdmreader.read_metadata(d['filename'])
                    if d['scan'] == sc.keys()[-1]:
                        print 'This job processed last scan of %s.' % d['filename']
                        #!!! todo: check that other scans are in finishedjobs. baseline assumption is that last scan finishes last

                        # check that BDFs are actually written (perhaps superfluous)
                        now = time.time()
                        print 'Waiting for all BDFs to be written for %s.' % d['filename']
                        while 1:
                            if all([sc[i]['bdfstr'] for i in sc.keys()]):
                                print 'All BDFs written for %s.' % d['filename']
                                break
                            elif time.time() - now > timeout:
                                print 'Timeout while waiting for BDFs in %s.' % d['filename']
                                break
                            else:
                                time.sleep(1)
                        
                        # do "end of SB" processing
                        # aggregate cands/noise files and make plots
                        status = subprocess.call(["queue_rtpipe.py", d['filename'], '--mode', 'cleanup'])
                        if status:
                            goodscans = count_candidates(os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl'))
                            status = subprocess.call(["queue_rtpipe.py", d['filename'], '--mode', 'plot_summary'])

                            #!!! if trig_arch:
                            # Get a sorted list of good scans, then convert it to a comma-delimited string to pass to choose_SDM_scans.pl
                            scanlist = goodscans.keys()
                            scanlist.sort() 
                            scanstring = ','.join(str(sc) for sc in scanlist)

                            # Edit SDM to remove no-cand scans. Perl script takes SDM work dir, and target directory to place edited SDM.
                            sdmArchdir = '/home/cbe-master/realfast/fake_archdir' #'/home/mctest/evla/sdm/' #!!! THIS NEEDS TO BE SET BY A CENTRALIZED SETUP/CONFIG FILE.
                            subprocess.call(['sdm_chop-n-serve.pl',d['filename'],d['workdir'],scanstring])

                            # NOW ARCHIVE EDITED SDM.
                            copyDirectory(os.path.join(d['workdir'],os.path.basename(d['filename'])),sdmArchdir)

                            #!!! Need to add a line here to clean up: remove SDM and edited SDM

                            #!!! Now archive the relevant BDFs for that SDM. Delete (or tag for deletion) the undesired BDFs.
 

                # remove from db
                removejob(jobid)

        # timeout tests? cleaning up jobs?
        sys.stdout.flush()
        time.sleep(1)

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
    if len(cands) == 0:
        print 'No cands found from %s.' % candsfile
        return (n.array([]), n.array([]))

    d = {}
    scans = [kk[0] for kk in cands.keys()]
    for scan in n.unique(scans):
        d[scan] = len(n.where(scan == scans)[0])

    return d

def copyDirectory(src, dest):
    try:
        shutil.copytree(src, dest)
    # Directories are the same
    except shutil.Error as e:
        print('Directory not copied. Error: %s' % e)
    # Any error saying that the directory doesn't exist
    except OSError as e:
        print('Directory not copied. Error: %s' % e)


def failed():
    """ Quick dump of trace for all failed jobs
    """

    q = Queue('failed', connection=conn0)
    print 'Failed queue:'
    print q.jobs
    for i in range(len(q.jobs)):
        print 'Failure %d' % i
        print q.jobs[i].exc_info
