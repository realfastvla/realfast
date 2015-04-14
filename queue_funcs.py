""" Functions imported by queue system.
"""

import os, glob, time
import sdmreader
import rtpipe.RT as rt
import rtpipe.calpipe as cp
import rtpipe.parsesdm as ps
import rtpipe.parsecands as pc
from rq import Queue, Connection
from redis import Redis

def read(workdir, filename, paramfile):
    """ Simple parse and return metadata for pipeline for first scan
    """

    sc, sr = sdmreader.read_metadata(os.path.join(workdir, filename))
    print
    print 'Scans, Target names:'
    print [(ss, sc[ss]['source']) for ss in sc]
    print
    print 'Example pipeline:'
    state = rt.set_pipeline(os.path.join(workdir, filename), sc.popitem()[0], paramfile=os.path.join(workdir, paramfile))

def search(qname, workdir, filename, paramfile, fileroot, scans, redishost='localhost', depends_on=''):
    """ Search for transients in all target scans and segments
    """

    # enqueue jobs
    states = []
    print 'Setting up pipelines for %s, scans %s...' % (filename, scans)
    for scan in scans:
        scanind = scans.index(scan)
        states.append(rt.set_pipeline(os.path.join(workdir, filename), scan, paramfile=os.path.join(workdir, paramfile), fileroot=fileroot))

    # submit to queue
    njobs = sum([states[i]['nsegments'] for i in range(len(states))])
    print 'Enqueuing %d jobs...' % (njobs)
    with Connection(Redis(redishost)):
        q = Queue(qname)

        # enqueue all but one
        for i in range(len(states)-1):
            for segment in range(states[i]['nsegments']):
                job = q.enqueue_call(func=rt.pipeline, args=(states[i], segment), depends_on=depends_on, timeout=24*3600, result_ttl=24*3600)

        # use second to last job as dependency for last job
        lastjob = q.enqueue_call(func=rt.pipeline, args=(states[-1], segment), depends_on=job, timeout=24*3600, result_ttl=24*3600)

    return lastjob

def calibrate(workdir, filename, fileroot):
    """ Run calibration pipeline
    """

    pipe = cp.pipe(os.path.join(workdir, filename), fileroot)
    pipe.run()

def calimg(workdir, filename, paramfile, scans):
    """ Search of a small segment of data without dedispersion.
    Intended to test calibration quality.
    """

    timescale = 1.  # average to this timescale (sec)
    joblist = []
    for scan in scans:
        state = ps.get_metadata(os.path.join(workdir, filename), scan)
        read_downsample = int(timescale/state['inttime'])
        state = rt.set_pipeline(os.path.join(workdir, filename), scan, paramfile=os.path.join(workdir, paramfile), nthread=1, nsegments=0, gainfile=os.path.join(workdir, gainfile), bpfile=os.path.join(workdir, bpfile), dmarr=[0], dtarr=[1], timesub='', candsfile='', noisefile='', read_downsample=read_downsample, fileroot=fileroot)
        joblist.append(q.enqueue_call(func=rt.pipeline, args=(state, state['nsegments']/2), timeout=24*3600, result_ttl=24*3600, depends_on=depends_on))  # image middle segment
    return joblist

def watch(workdir, filename):
    """ Watch a directory for a new file with subname in its name.
    Meant to be real-time queue trigger
    """

    filelist0 = os.listdir(os.path.abspath(workdir))
    while 1:
        filelist = os.listdir(os.path.abspath(workdir))
        newfiles = [ff for ff in filelist if ff not in filelist0]
        matchfiles = filter(lambda newfile: filename in newfile, newfiles)
        if len(matchfiles):
            if len(matchfiles) > 1:
                print 'More than one match!', matchfiles
            else:
                break

        filelist0 = filelist
        time.sleep(1)
    return matchfiles[0]

def cleanup(workdir, fileroot, scans):
    """ Cleanup up noise and cands files.
    Finds all segments in each scan and merges them into single cand/noise file per scan.
    """

    # merge cands files
    for scan in scans:
        try:
            pkllist = glob.glob(os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + 'seg*.pkl'))
            pc.merge_segments(pkllist, fileroot + '_sc' + str(scan))
        except AssertionError:
            print 'No cands files found for scan %d' % scan

        if os.path.exists(os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl')):
            for cc in pkllist:
                os.remove(cc)

        # merge noise files
        try:
            pkllist = glob.glob(os.path.join(workdir, 'noise_' + fileroot + '_sc' + str(scan) + 'seg*.pkl'))
            pc.merge_segments(pkllist, fileroot + '_sc' + str(scan))
        except AssertionError:
            print 'No noise files found for scan %d' % scan

        if os.path.exists(os.path.join(workdir, 'noise_' + fileroot + '_sc' + str(scan) + '.pkl')):
            for cc in pkllist:
                os.remove(cc)

def plot_summary(workdir, fileroot, scans):
    """ Make summary plots.
    pkllist gives list of cand pkl files for visualization.
    default mode is to make cand and noise summary plots
    """

    pkllist = []
    for scan in scans:
        pklfile = os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl')
        if os.path.exists(pklfile):
            pkllist.append(pklfile)
    pc.plot_summary(pkllist)
    
    pkllist = []
    for scan in scans:
        pklfile = os.path.join(workdir, 'noise_' + fileroot + '_sc' + str(scan) + '.pkl')
        if os.path.exists(pklfile):
            pkllist.append(pklfile)
    pc.plot_noise(pkllist)

def plot_cand(workdir, fileroot, scans, candnum=-1):
    """ Visualize a candidate
    """

    pkllist = []
    for scan in scans:
        pklfile = os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl')
        if os.path.exists(pklfile):
            pkllist.append(pklfile)

    pc.plot_cand(pkllist, candnum=candnum)

def plot_pulsar(fileroot, scans):
    """
    Assumes 3 or 4 input pulsar scans (centered then offset pointings).
    """

    pkllist = []
    for scan in scans:
        pkllist.append(os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl'))

    pc.plot_psrrates(pkllist, outname='plot_' + fileroot + '_psrrates.png')


def joblistwait(qname, jobids, redishost='localhost'):
    """ Function listens to jobs of jobids on q. Completes only when all jobs have is_finished status=True.
    """

    print 'Monitoring %d jobs on queue %s' % (len(jobids), qname)
    with Connection(Redis(redishost)):
        q = Queue(qname)

        while len(jobids):
            for jobid in jobids:
                job = q.fetch_job(jobid)
                if job.is_finished:
                    jobids.remove(jobid)
                    print 'Jobid %s finished. %d remain.' % (jobid, len(jobids))
                else:
                    print '.',
                    time.sleep(5)
