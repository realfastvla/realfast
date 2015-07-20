from redis import Redis
from rq.queue import Queue
from rq.registry import FinishedJobRegistry
import time
import sdmreader
from rtpipe.parsecands import count_candidates
import rtpipe.RT as rt

conn = Redis(db=1)   # db for tracking ids of tail jobs
timeout = 600   # seconds to wait for BDF to finish writing (after final pipeline job completes)

def monitor(qname='default'):
    """ Blocking loop that prints the jobs currently being tracked.
    """

    q = Queue(qname, connection=conn)

    while 1:
        jobids = conn.scan()[1]
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
                        # aggregate cands/noise files and make plots
                        status = subprocess.call(["queue_rtpipe.py", d['filename'], '--mode', 'cleanup'])
                        if status:
                            goodscans = count_candidates(os.path.join(d['workdir'], 'cands_' + d['fileroot'] + '_merge.pkl'))
                            status = subprocess.call(["queue_rtpipe.py", d['filename'], '--mode', 'plot_summary'])

                            # scan/chop SDM. 'goodscans' defines scans to archive.

                            # copy new SDM and good BDFs to archive locations (new stuff)

                # remove from db
                removejob(jobid)

        # timeout tests? cleaning up jobs?
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

    conn0 = Redis(db=0)
    q = Queue(qname, connection=conn0)
    return FinishedJobRegistry(name=q.name, connection=conn0).get_job_ids()

if __name__ == '__main__':
    monitor()
