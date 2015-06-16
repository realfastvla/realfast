""" Functions imported by queue system.
"""

import os, glob, time, shutil, subprocess
import sdmreader
import rtpipe.RT as rt
import rtpipe.calpipe as cp
import rtpipe.parsesdm as ps
import rtpipe.parsecands as pc
from rq import Queue, Connection
from redis import Redis

def read(filename, paramfile=''):
    """ Simple parse and return metadata for pipeline for first scan
    """

    sc, sr = sdmreader.read_metadata(filename)
    print
    print 'Scans, Target names:'
    print [(ss, sc[ss]['source']) for ss in sc]
    print
    print 'Example pipeline:'
    state = rt.set_pipeline(filename, sc.popitem()[0], paramfile=paramfile, nologfile=True)

def search(qname, filename, paramfile, fileroot, scans=[], redishost='localhost', depends_on=None):
    """ Search for transients in all target scans and segments
    """

    # enqueue jobs
    stateseg = []
    print 'Setting up pipelines for %s, scans %s...' % (filename, scans)
    for scan in scans:
        scanind = scans.index(scan)
        state = rt.set_pipeline(filename, scan, paramfile=paramfile, fileroot=fileroot)
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

def calibrate(filename, fileroot):
    """ Run calibration pipeline
    """

    pipe = cp.pipe(filename, fileroot)
    pipe.run()

def calimg(filename, paramfile, scans=[]):
    """ Search of a small segment of data without dedispersion.
    Intended to test calibration quality.
    """

    timescale = 1.  # average to this timescale (sec)
    joblist = []
    for scan in scans:
        state = ps.get_metadata(filename, scan)
        read_downsample = int(timescale/state['inttime'])
        state = rt.set_pipeline(filename, scan, paramfile=paramfile, nthread=1, nsegments=0, gainfile=gainfile, bpfile=bpfile, dmarr=[0], dtarr=[1], timesub='', candsfile='', noisefile='', read_downsample=read_downsample, fileroot=fileroot)
        joblist.append(q.enqueue_call(func=rt.pipeline, args=(state, state['nsegments']/2), timeout=24*3600, result_ttl=24*3600, depends_on=depends_on))  # image middle segment
    return joblist

def lookalldaemon(lookdir, subname, workdir, paramfile, fileroot, qname='default', redishost='localhost', newonly=True):
    """ Function to look at lookdir for subname. 
    Each file found is started through entire calibration/search/visualization pipeline.
    Uses gevent to package up queue submission and allow daemonization of file wait.
    """

    import gevent

    print 'Looking for %s in %s.' % (subname, lookdir)

    filelist0 = os.listdir(os.path.abspath(lookdir))
    joblist = []
    completedlist = []
    try:
        while 1:
            filelist = os.listdir(os.path.abspath(lookdir))
            if newonly:
                newfiles = filter(lambda ff: ff not in filelist0, filelist)
                matchfiles = filter(lambda ff: subname in ff, newfiles)
            else:
                matchfiles = filter(lambda ff: subname in ff, filelist)
            matchfilestodo = filter(lambda ff: ff not in completedlist, matchfiles)   # remove files already completed

            for filename in matchfilestodo:
                fullfilename = os.path.join(lookdir, filename)
                job = gevent.spawn(runall, fullfilename, workdir, paramfile, fileroot, qname, redishost)
                job.run()
                joblist.append(job)
                completedlist.append(filename)

            filelist0 = filelist
            time.sleep(1)
    except KeyboardInterrupt:
        print 'Breaking out. Total of %d jobs spawned.' % len(joblist)
#        gevent.joinall(joblist)

def runall(filename, workdir, paramfile, fileroot, qname='default', redishost='localhost'):
    """ Runs all major steps on qname
    """

    with Connection(Redis(redishost)):
        q = Queue(qname, async=False)

        sdmjob = q.enqueue_call(func=waitforsdm, args=(filename,), timeout=24*3600, result_ttl=24*3600)

        # copy to working area and make cal-able
        newfileloc = os.path.join(workdir, os.path.split(filename)[1])
        if not os.path.exists(newfileloc):
            shutil.copytree(filename, newfileloc)  # copy file in
        else:
            print 'File %s already in %s. Using that one...' % (newfileloc, workdir)
        filename = newfileloc
        sdmcaljob = q.enqueue_call(func=sdmascal, args=(filename,), timeout=24*3600, result_ttl=24*3600)   # make emtpy sdm workable as cal sdm
        caljob = q.enqueue_call(func=calibrate, args=(filename, fileroot), timeout=24*3600, result_ttl=24*3600)   # can be set to enqueue when data arrives
        sdmorigjob = q.enqueue_call(func=sdmasorig, args=(filename,), timeout=24*3600, result_ttl=24*3600, depends_on=caljob)   # make emtpy sdm workable to search

        # add a step to fill bdfpkls?

        # start non-blocking part
#        q = Queue(qname)
#        scans = getscans(filename, sources=sources, scans=scans, intent='TARGET')  # default cleans up target scans
#        lastsearchjob = search(q.name, filename, paramfile, fileroot, scans=scans, redishost=redishost, depends_on=sdmorigjob)
#        cleanjob = q.enqueue_call(func=cleanup, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=lastsearchjob)  # enqueued when joblist finishes
#        plotjob = q.enqueue_call(func=plot_summary, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=cleanjob)   # enqueued when cleanup finished

def lookforfile(lookdir, subname, changesonly=False):
    """ Look for and return a file with subname in lookdir.
    changesonly means it will wait for changes. default looks only once.
    """

    print 'Looking for %s in %s.' % (subname, lookdir)

    filelist0 = os.listdir(os.path.abspath(lookdir))
    if changesonly:
        while 1:
            filelist = os.listdir(os.path.abspath(lookdir))
            newfiles = filter(lambda ff: ff not in filelist0, filelist)
            matchfiles = filter(lambda ff: subname in ff, newfiles)
            if len(matchfiles):
                break
            else:
                print '.',
                filelist0 = filelist
                time.sleep(1)
    else:
        matchfiles = filter(lambda ff: subname in ff, filelist0)

    if len(matchfiles) == 0:
        print 'No file found.'
        fullname = ''
    elif len(matchfiles) == 1:
        fullname = os.path.join(lookdir, matchfiles[0])
    elif len(matchfiles) > 1:
        print 'More than one match!', matchfiles,
        fullname = os.path.join(lookdir, matchfiles[0])

    print 'Returning %s.' % fullname
    return fullname

def waitforsdm(filename, timeout=300):
    """ Monitors filename (an SDM) to see when it is finished writing.
    timeout is time in seconds to wait from first detection of file.
    Intended for use on CBE.
    """

    time_filestart = 0
    while 1:
        try:
            sc,sr = sdmreader.read_metadata(filename)
        except RuntimeError:
            print 'File %s not found.' % filename
        except IOError:
            print 'File %s does not have Antenna.xml yet...' % filename
            time.sleep(2)
            continue
        else:
            bdflocs = [sc[ss]['bdfstr'] for ss in sc]
            if not time_filestart:
                print 'File %s exists. Waiting for it to complete writing.' % filename
                time_filestart = time.time()

        # if any bdfstr are not set, then file not finished
        if None in bdflocs:
            if time.time() - time_filestart < timeout:
                print 'bdfs not all written yet. Waiting...'
                time.sleep(2)
                continue
            else:
                print 'Timeout exceeded. Exiting...'
                break
        else:
            print 'All bdfs written. Continuing.'
            break

def sdmascal(filename, calscans='', bdfdir='/lustre/evla/wcbe/data/bunker'):
    """ Takes incomplete SDM (on CBE) and creates one corrected for use in calibration.
    optional calscans is casa-like string to select scans
    """

    if not calscans:
        calscanlist = getscans(filename, intent='CALI')   # get calibration scans
        calscans = ','.join([str(sc) for sc in calscanlist])   # put into CASA-like selection string

    # make new Main.xml for relevant scans
    if ( os.path.exists(os.path.join(filename, 'Main_cal.xml')) and os.path.exists(os.path.join(filename, 'ASDMBinary_cal')) ):
        print 'Found existing cal xml and data. Moving in...'
        shutil.copyfile(os.path.join(filename, 'Main.xml'), os.path.join(filename, 'Main_orig.xml'))   # put new Main.xml in
        shutil.move(os.path.join(filename, 'Main_cal.xml'), os.path.join(filename, 'Main.xml'))   # put new Main.xml in
        shutil.move(os.path.join(filename, 'ASDMBinary_cal'), os.path.join(filename, 'ASDMBinary'))
    else:
        shutil.copyfile(os.path.join(filename, 'Main.xml'), os.path.join(filename, 'Main_orig.xml'))   # put new Main.xml in
        subprocess.call(['choose_SDM_scans.pl', filename, os.path.join(filename, 'Main_cal.xml'), calscans])  #  modify Main.xml
        shutil.move(os.path.join(filename, 'Main_cal.xml'), os.path.join(filename, 'Main.xml'))   # put new Main.xml in

        if not os.path.exists(os.path.join(filename, 'ASDMBinary')):
            os.makedirs(os.path.join(filename, 'ASDMBinary'))

        sc,sr = sdmreader.read_metadata(filename)
        for calscan in calscanlist:
            bdfstr = sc[calscan]['bdfstr'].split('/')[-1]
            bdffile = glob.glob(os.path.join(bdfdir, bdfstr))[0]
            bdffiledest = os.path.join(filename, 'ASDMBinary', os.path.split(bdffile)[1])
            if not os.path.exists(bdffiledest):
                print 'Copying bdf in for calscan %d.' % calscan
                shutil.copyfile(bdffile, bdffiledest)
            else:
                print 'bdf in for calscan %d already in place.' % calscan            

def sdmasorig(filename):
    """ Take sdm for calibration and restore it.
    keeps ASDMBinary around, just in case
    """

    shutil.move(os.path.join(filename, 'Main.xml'), os.path.join(filename, 'Main_cal.xml'))
    shutil.move(os.path.join(filename, 'Main_orig.xml'), os.path.join(filename, 'Main.xml'))
    shutil.move(os.path.join(filename, 'ASDMBinary'), os.path.join(filename, 'ASDMBinary_cal'))

def cleanup(workdir, fileroot, scans=[]):
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

def plot_summary(workdir, fileroot, scans=[]):
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

def plot_cand(workdir, fileroot, scans=[], candnum=-1):
    """ Visualize a candidate
    """

    pkllist = []
    for scan in scans:
        pklfile = os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl')
        if os.path.exists(pklfile):
            pkllist.append(pklfile)

    pc.plot_cand(pkllist, candnum=candnum)

def plot_pulsar(workdir, fileroot, scans=[]):
    """
    Assumes 3 or 4 input pulsar scans (centered then offset pointings).
    """

    pkllist = []
    for scan in scans:
        pkllist.append(os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl'))

    print 'Pulsar plotting for pkllist:', pkllist
    pc.plot_psrrates(pkllist, outname=os.path.join(workdir, 'plot_' + fileroot + '_psrrates.png'))

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

def getscans(filename, scans='', sources='', intent=''):
    """ Get scan list as ints.
    First tries to parse scans, then sources, then intent.
    """

    # if no scans defined, set by mode context
    if scans:
        scans = [int(i) for i in scans.split(',')]
    elif sources:
        meta = sdmreader.read_metadata(filename)

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
        meta = sdmreader.read_metadata(filename)
        scans = filter(lambda sc: intent in meta[0][sc]['intent'], meta[0].keys())
#        scans = [sc for sc in meta[0].keys() if intent in meta[0][sc]['intent']]   # get all target fields
    else:
        print 'Must provide scans, sources, or intent.'
        raise BaseException

    return scans
