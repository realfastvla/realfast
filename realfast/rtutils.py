""" Functions imported by queue system.
"""

import os, glob, time, shutil, subprocess, logging
import sdmreader
import rtpipe.RT as rt
import rtpipe.calpipe as cp
import rtpipe.parsesdm as ps
import rtpipe.parsecands as pc
from rq import Queue, Connection
from redis import Redis

logging.basicConfig(format="%(asctime)-15s %(levelname)8s %(message)s", level=logging.INFO)

def read(filename, paramfile='', fileroot='', bdfdir='/lustre/evla/wcbe/data/realfast'):
    """ Simple parse and return metadata for pipeline for first scan
    """

    sc, sr = sdmreader.read_metadata(filename, bdfdir=bdfdir)
    logging.info('Scans, Target names:')
    logging.info('%s' % str([(ss, sc[ss]['source']) for ss in sc]))
    logging.info('Example pipeline:')
    state = rt.set_pipeline(filename, sc.popitem()[0], paramfile=paramfile, fileroot=fileroot, nologfile=True)

def search(qname, filename, paramfile, fileroot, scans=[], telcalfile='', redishost='localhost', depends_on=None):
    """ Search for transients in all target scans and segments
    """

    # enqueue jobs
    stateseg = []
    logging.info('Setting up pipelines for %s, scans %s...' % (filename, scans))

    for scan in scans:
        assert isinstance(scan, int)
        scanind = scans.index(scan)
        state = rt.set_pipeline(filename, scan, paramfile=paramfile, fileroot=fileroot, gainfile=telcalfile, writebdfpkl=True, nologfile=True)
        for segment in grouprange(0, state['nsegments'], 3):   # submit three segments at a time to reduce read/prep overhead
            stateseg.append( (state, segment) )
    njobs = len(stateseg)

    if njobs:
        logging.info('Enqueuing %d job%s...' % (njobs, 's'[:njobs-1]))

        # submit to queue
        with Connection(Redis(redishost)):
            q = Queue(qname)

            # enqueue all but one
            if njobs > 1:
                for i in range(njobs-1):
                    state, segment = stateseg[i]
                    job = q.enqueue_call(func=rt.pipeline, args=(state, segment), depends_on=depends_on, timeout=6*3600, result_ttl=24*3600)
            else:
                job = None

            # use second to last job as dependency for last job
            state, segment = stateseg[-1]
            lastjob = q.enqueue_call(func=rt.pipeline, args=(state, segment), depends_on=job, at_front=True, timeout=6*3600, result_ttl=24*3600)  # queued after others, but moved to front of queue

        logging.info('Jobs enqueued. Returning last job with id %s.' % lastjob.id)
        return lastjob
    else:
        logging.info('No jobs to enqueue')
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

def gettelcalfile(telcaldir, filename, timeout=0):
    """ Looks for telcal file with name filename.GN in typical telcal directories
    Searches recent directory first, then tries tree search.
    If none found and timeout=0, returns empty string. Else will block for timeout seconds.
    """

    fname = os.path.basename(filename)
    time_filestart = time.time()

    # search for associated telcal file
    year = str(time.localtime()[0])
    month = '%02d' % time.localtime()[1]
    telcaldir2 = os.path.join(telcaldir, year, month)

    while 1:
        logging.info('Looking for telcalfile in %s' % telcaldir2)
        telcalfile = [os.path.join(telcaldir2, ff) for ff in os.listdir(telcaldir2) if fname+'.GN' in ff]
        
        # if not in latest directory, walk through whole structure
        if not len(telcalfile):
            logging.info('No telcal in newest directory. Searching whole telcalfile tree.')
            telcalfile = [os.path.join(root, fname+'.GN') for root, dirs, files in os.walk(telcaldir) if fname+'.GN' in files]

        assert isinstance(telcalfile, list)

        # make into string (emtpy or otherwise)
        if len(telcalfile) == 1:
            telcalfile = telcalfile[0]
            logging.info('Found telcal file at %s' % telcalfile)
        elif len(telcalfile) > 1:
            telcalfile = ''
            logging.info('Found multiple telcalfiles %s' % telcalfile)
        else:
            telcalfile = ''
            logging.info('No telcal file found in %s' % telcaldir)

        assert isinstance(telcalfile, str)

        # if waiting, but no file found, check timeout
        if timeout and not telcalfile:
            if time.time() - time_filestart < timeout:  # don't beak yet
                logging.info('Waiting for telcalfile...')
                time.sleep(2)
                continue
            else:   # reached timeout
                logging.info('Timeout waiting for telcalfile')
                break
        else:
            logging.info('Not waiting for telcalfile')
            break

    return telcalfile

def rsyncsdm(filename, workdir):
    """ Uses subprocess.call to call rsync for filename into workdir.
    """

    subprocess.call(["rsync", "-av", filename.rstrip('/'), workdir])

def copysdm(filename, workdir):
    """ Copies sdm from filename (full path) to workdir
    """

    # first copy data to working area
    fname = os.path.basename(filename)
    newfileloc = os.path.join(workdir, fname)
    if not os.path.exists(newfileloc):
        logging.info('Copying %s into %s' % (fname, workdir))
        shutil.copytree(filename, newfileloc)  # copy file in
    else:
        logging.info('File %s already in %s. Using that one...' % (fname, workdir))
    filename = newfileloc

def lookforfile(lookdir, subname, changesonly=False):
    """ Look for and return a file with subname in lookdir.
    changesonly means it will wait for changes. default looks only once.
    """

    logging.info('Looking for %s in %s.' % (subname, lookdir))

    filelist0 = os.listdir(os.path.abspath(lookdir))
    if changesonly:
        while 1:
            filelist = os.listdir(os.path.abspath(lookdir))
            newfiles = filter(lambda ff: ff not in filelist0, filelist)
            matchfiles = filter(lambda ff: subname in ff, newfiles)
            if len(matchfiles):
                break
            else:
                logging.info('.')
                filelist0 = filelist
                time.sleep(1)
    else:
        matchfiles = filter(lambda ff: subname in ff, filelist0)

    if len(matchfiles) == 0:
        logging.info('No file found.')
        fullname = ''
    elif len(matchfiles) == 1:
        fullname = os.path.join(lookdir, matchfiles[0])
    elif len(matchfiles) > 1:
        logging.info('More than one match!', matchfiles)
        fullname = os.path.join(lookdir, matchfiles[0])

    logging.info('Returning %s.' % fullname)
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
            logging.info('File %s not found.' % filename)
        except IOError:
            logging.info('File %s does not have Antenna.xml yet...' % filename)
            time.sleep(2)
            continue
        else:
            bdflocs = [sc[ss]['bdfstr'] for ss in sc]
            if not time_filestart:
                logging.info('File %s exists. Waiting for it to complete writing.' % filename)
                time_filestart = time.time()

        # if any bdfstr are not set, then file not finished
        if None in bdflocs:
            if time.time() - time_filestart < timeout:
                logging.info('bdfs not all written yet. Waiting...')
                time.sleep(2)
                continue
            else:
                logging.info('Timeout exceeded. Exiting...')
                break
        else:
            logging.info('All bdfs written. Continuing.')
            break

def sdmascal(filename, calscans='', bdfdir='/lustre/evla/wcbe/data/realfast'):
    """ Takes incomplete SDM (on CBE) and creates one corrected for use in calibration.
    optional calscans is casa-like string to select scans
    """

    if not calscans:
        calscanlist = getscans(filename, intent='CALI')   # get calibration scans
        calscans = ','.join([str(sc) for sc in calscanlist])   # put into CASA-like selection string

    # make new Main.xml for relevant scans
    if ( os.path.exists(os.path.join(filename, 'Main_cal.xml')) and os.path.exists(os.path.join(filename, 'ASDMBinary_cal')) ):
        logging.info('Found existing cal xml and data. Moving in...')
        shutil.copyfile(os.path.join(filename, 'Main.xml'), os.path.join(filename, 'Main_orig.xml'))   # put new Main.xml in
        shutil.move(os.path.join(filename, 'Main_cal.xml'), os.path.join(filename, 'Main.xml'))   # put new Main.xml in
        shutil.move(os.path.join(filename, 'ASDMBinary_cal'), os.path.join(filename, 'ASDMBinary'))
    else:
        shutil.copyfile(os.path.join(filename, 'Main.xml'), os.path.join(filename, 'Main_orig.xml'))   # put new Main.xml in
        subprocess.call(['choose_SDM_scans.pl', filename, os.path.join(filename, 'Main_cal.xml'), calscans])  #  modify Main.xml
        shutil.move(os.path.join(filename, 'Main_cal.xml'), os.path.join(filename, 'Main.xml'))   # put new Main.xml in

        if not os.path.exists(os.path.join(filename, 'ASDMBinary')):
            os.makedirs(os.path.join(filename, 'ASDMBinary'))

        sc,sr = sdmreader.read_metadata(filename, bdfdir=bdfdir)
        for calscan in calscanlist:
            bdfstr = sc[calscan]['bdfstr'].split('/')[-1]
            bdffile = glob.glob(os.path.join(bdfdir, bdfstr))[0]
            bdffiledest = os.path.join(filename, 'ASDMBinary', os.path.split(bdffile)[1])
            if not os.path.exists(bdffiledest):
                logging.info('Copying bdf in for calscan %d.' % calscan)
                shutil.copyfile(bdffile, bdffiledest)
            else:
                logging.info('bdf in for calscan %d already in place.' % calscan)

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
            logging.info('No cands files found for scan %d' % scan)

        if os.path.exists(os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl')):
            for cc in pkllist:
                os.remove(cc)

        # merge noise files
        try:
            pkllist2 = glob.glob(os.path.join(workdir, 'noise_' + fileroot + '_sc' + str(scan) + 'seg*.pkl'))
            pc.merge_segments(pkllist2)
        except AssertionError:
            logging.info('No noise files found for scan %d' % scan)

        if os.path.exists(os.path.join(workdir, 'noise_' + fileroot + '_sc' + str(scan) + '.pkl')):
            for cc in pkllist2:
                os.remove(cc)

        if len(pkllist) != len(pkllist2):
            logging.info('Uh oh. noise and cands pkl lists not identical. Missing pkl?')

def plot_summary(workdir, fileroot, scans=[], remove=[]):
    """ Make summary plots.
    pkllist gives list of cand pkl files for visualization.
    default mode is to make cand and noise summary plots
    """

    pkllist = []
    for scan in scans:
        pklfile = os.path.join(workdir, 'cands_' + fileroot + '_sc' + str(scan) + '.pkl')
        if os.path.exists(pklfile):
            pkllist.append(pklfile)
    pc.plot_summary(pkllist, remove=remove)
    
    pkllist = []
    for scan in scans:
        pklfile = os.path.join(workdir, 'noise_' + fileroot + '_sc' + str(scan) + '.pkl')
        if os.path.exists(pklfile):
            pkllist.append(pklfile)
    pc.plot_noise(pkllist, remove=remove)

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

    logging.info('Pulsar plotting for pkllist:', pkllist)
    pc.plot_psrrates(pkllist, outname=os.path.join(workdir, 'plot_' + fileroot + '_psrrates.png'))

def getscans(filename, scans='', sources='', intent='', bdfdir='/lustre/evla/wcbe/data/realfast'):
    """ Get scan list as ints.
    First tries to parse scans, then sources, then intent.
    """

    # if no scans defined, set by mode context
    if scans:
        scans = [int(i) for i in scans.split(',')]
    elif sources:
        meta = sdmreader.read_metadata(filename, bdfdir=bdfdir)

        # if source list provided, parse it then append all scans to single list
        sources = [i for i in sources.split(',')]
        scans = []
        for source in sources:
            sclist = filter(lambda sc: source in meta[0][sc]['source'], meta[0].keys())
#            sclist = [sc for sc in meta[0].keys() if source in meta[0][sc]['source']]
            if len(sclist):
                scans += sclist
            else:
                logging.info('No scans found for source %s' % source)
    elif intent:
        meta = sdmreader.read_metadata(filename, bdfdir=bdfdir)
        scans = filter(lambda sc: intent in meta[0][sc]['intent'], meta[0].keys())
#        scans = [sc for sc in meta[0].keys() if intent in meta[0][sc]['intent']]   # get all target fields
    else:
        logging.error('Must provide scans, sources, or intent.')
        raise BaseException

    return scans

def grouprange(start, size, step):
    arr = range(start,start+size)
    return [arr[ss:ss+step] for ss in range(0, len(arr), step)]
