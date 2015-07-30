from redis import Redis
from rq.queue import Queue
from rq.registry import FinishedJobRegistry
import time, pickle, sys
import sdmreader
import click
from realfast import rtutils

conn0 = Redis(db=0)
conn = Redis(db=1)   # db for tracking ids of tail jobs
timeout = 600   # seconds to wait for BDF to finish writing (after final pipeline job completes)

@click.command()
@click.option('--qname', default='default', help='Name of queue to monitor')
@click.option('--triggered/--all', '-t', default=False, help='Triggered recording of scans or save all? (default: all)')
def monitor(qname, triggered):
    """ Blocking loop that prints the jobs currently being tracked in queue 'qname'.
    Can optionally be set to do triggered data recording (archiving).
    """

    print 'Monitoring queue %s in %s recording mode...' % (qname, ['all', 'triggered'][triggered])
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
                # todo: check that all other segmentss are also finished? baseline assumption is that all segments finish before this one.
#                finishedjobs = getfinishedjobs(qname)

                # is this the last scan of sdm?
                if 'RT.pipeline' in job.func_name:
                    d, segments = job.args
                    sc,sr = sdmreader.read_metadata(d['filename'])
                    if d['scan'] == sc.keys()[-1]:
                        print 'This job processed last scan of %s.' % d['filename']
                        # todo: check that other scans are in finishedjobs. baseline assumption is that last scan finishes last

                        # check that BDFs are actually written (perhaps superfluous)
                        now = time.time()
                        print 'Waiting for all BDF to be written for %s.' % d['filename']
                        while 1:
                            if all([sc[i]['bdfstr'] for i in sc.keys()]):
                                print 'All BDF written for %s.' % d['filename']
                                break
                            elif time.time() - now > timeout:
                                print 'Timeout while waiting for BDFs in %s.' % d['filename']
                                break
                            else:
                                time.sleep(1)
                        
                        # do "end of SB" processing
                        # 1) aggregate cands/noise files
                        rtutils.cleanup(d['workdir'], d['fileroot'], sc.keys())

                        # 2) if triggered recording, get scans with detections, else save all.
                        if triggered:  
                            goodscans = count_candidates(os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl'))
                        else:
                            goodscans = sc.keys()

                        # 3) scan/chop SDM. 'goodscans' defines scans to archive.

                        # 4) copy new SDM and good BDFs to archive locations (new stuff)

                        # 5) finally plot candidates
                        rtutils.plot_summary(d['workdir'], d['fileroot'], sc.keys())

                        # 6) do some clean up of cands/noise files

                # job is finished, so remove from db
                removejob(jobid)

        # timeout tests?
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

    scans = [kk[0] for kk in cands.keys()]
    for scan in list(set(scans)):   # unique scans
        d[scan] = len(n.where(scan == scans)[0])

    return d

def failed():
    """ Quick dump of trace for all failed jobs
    """

    q = Queue('failed', connection=conn0)
    print 'Failed queue:'
    print q.jobs
    for i in range(len(q.jobs)):
        print 'Failure %d' % i
        print q.jobs[i].exc_info
