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

def search(qname, workdir, filename, paramfile, fileroot, sources='', scans='', redishost='localhost', depends_on=None):
    """ Search for transients in all target scans and segments
    """

    # enqueue jobs
    stateseg = []
    print 'Setting up pipelines for %s, scans %s...' % (filename, scans)
    for scan in scans:
        scanind = scans.index(scan)
        state = rt.set_pipeline(os.path.join(workdir, filename), scan, paramfile=os.path.join(workdir, paramfile), fileroot=fileroot)
        for segment in range(state['nsegments']):
            stateseg.append( (state, segment) )
    njobs = len(stateseg)

    if njobs:
        print 'Enqueuing %d jobs...' % (njobs)

        # submit to queue
        with Connection(Redis(redishost)):
            q = Queue(qname)

            # enqueue all but one
            for i in range(njobs-1):
                state, segment = stateseg[i]
                job = q.enqueue_call(func=rt.pipeline, args=(state, segment), depends_on=depends_on, timeout=24*3600, result_ttl=24*3600)

            # use second to last job as dependency for last job
            state, segment = stateseg[-1]
            lastjob = q.enqueue_call(func=rt.pipeline, args=(state, segment), depends_on=job, timeout=24*3600, result_ttl=24*3600)
        return lastjob
    else:
        print 'No jobs to enqueue'
        return

def calibrate(workdir, filename, fileroot):
    """ Run calibration pipeline
    """

    pipe = cp.pipe(os.path.join(workdir, filename), fileroot)
    pipe.run()

def calimg(workdir, filename, paramfile, sources='', scans=''):
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

def waitfor(workdir, filename):
    """ Wait for a new file with subname in workdir.
    """

    filelist0 = os.listdir(os.path.abspath(workdir))
    print 'Waiting for %s.' % filename,
    while 1:
        filelist = os.listdir(os.path.abspath(workdir))
        newfiles = filter(lambda ff: ff not in filelist0, filelist)
        matchfiles = filter(lambda newfile: filename in newfile, newfiles)
        if len(matchfiles):
            if len(matchfiles) > 1:
                print 'More than one match! Using first.', matchfiles
                break
            else:
                break
        else:
            print '.',

        filelist0 = filelist
        time.sleep(1)
    return matchfiles[0]

def lookfor(workdir, filename):
    """ Look for and return a file in workdir.
    """

    print 'Looking for %s.' % filename,
    while 1:
        matchfiles = filter(lambda ff: filename in ff, os.listdir(os.path.abspath(workdir)))

        if len(matchfiles):
            if len(matchfiles) > 1:
                print 'More than one match! Using first.', matchfiles
                break
            else:
                break
        else:
            print '.'

        time.sleep(1)
    return matchfiles[0]

def cleanup(workdir, fileroot, sources='', scans=''):
    """ Cleanup up noise and cands files.
    Finds all segments in each scan and merges them into single cand/noise file per scan.
    """

    # merge cands files
    for scan in scans:
        try:
            pkllist = glob.glob(os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + 'seg*.pkl'))
            pc.merge_segments(pkllist)
        except AssertionError:
            print 'No cands files found for scan %d' % scan

        if os.path.exists(os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl')):
            for cc in pkllist:
                os.remove(cc)

        # merge noise files
        try:
            pkllist = glob.glob(os.path.join(workdir, 'noise_' + fileroot + '_sc' + str(scan) + 'seg*.pkl'))
            pc.merge_segments(pkllist)
        except AssertionError:
            print 'No noise files found for scan %d' % scan

        if os.path.exists(os.path.join(workdir, 'noise_' + fileroot + '_sc' + str(scan) + '.pkl')):
            for cc in pkllist:
                os.remove(cc)

def plot_summary(workdir, fileroot, sources='', scans=''):
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

def plot_cand(workdir, fileroot, sources='', scans='', candnum=-1):
    """ Visualize a candidate
    """

    pkllist = []
    for scan in scans:
        pklfile = os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl')
        if os.path.exists(pklfile):
            pkllist.append(pklfile)

    pc.plot_cand(pkllist, candnum=candnum)

def plot_pulsar(fileroot, sources='', scans=''):
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

def getscans(workdir, filename, scans='', sources='', intent=''):
    """ Get scan list as ints.
    First tries to parse scans, then sources, then intent.
    """

    # if no scans defined, set by mode context
    if scans:
        scans = [int(i) for i in scans.split(',')]
    elif sources:
        meta = sdmreader.read_metadata(os.path.join(workdir, filename))

        # if source list provided, parse it then append all scans to single list
        sources = [i for i in sources.split(',')]
        scans = []
        for source in sources:
            sclist = filter(lambda sc: source in meta[0][sc]['source'], meta[0].keys())
#            sclist = [sc for sc in meta[0].keys() if source in meta[0][sc]['source']]
            if len(sclist):
                scans += sclist
            else:
                print 'No scans found for source %s' % source
    elif intent:
        meta = sdmreader.read_metadata(os.path.join(workdir, filename))
        scans = filter(lambda sc: intent in meta[0][sc]['intent'], meta[0].keys())
#        scans = [sc for sc in meta[0].keys() if intent in meta[0][sc]['intent']]   # get all target fields
    else:
        print 'Must provide scans, sources, or intent.'
        raise BaseException

    return scans
