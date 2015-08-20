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

logger = logging.getLogger(__name__)

def read(filename, paramfile='', fileroot='', bdfdir='/lustre/evla/wcbe/data/realfast'):
    """ Simple parse and return metadata for pipeline for first scan
    """

    sc, sr = sdmreader.read_metadata(filename, bdfdir=bdfdir)
    logger.info('Scans, Target names:')
    logger.info('%s' % str([(ss, sc[ss]['source']) for ss in sc]))
    logger.info('Example pipeline:')
    state = rt.set_pipeline(filename, sc.popitem()[0], paramfile=paramfile, fileroot=fileroot, nologfile=True)

def search(qname, filename, paramfile, fileroot, scans=[], telcalfile='', redishost='localhost', depends_on=None, bdfdir='/lustre/evla/wcbe/data/bunker'):
    """ Search for transients in all target scans and segments
    """

    # enqueue jobs
    stateseg = []
    logger.info('Setting up pipelines for %s, scans %s...' % (filename, scans))

    for scan in scans:
        assert isinstance(scan, int), 'Scan should be an integer'
        scanind = scans.index(scan)
        state = rt.set_pipeline(filename, scan, paramfile=paramfile, fileroot=fileroot, gainfile=telcalfile, writebdfpkl=True, nologfile=True, bdfdir=bdfdir)
        for segment in grouprange(0, state['nsegments'], 3):   # submit three segments at a time to reduce read/prep overhead
            stateseg.append( (state, segment) )
    njobs = len(stateseg)

    if njobs:
        logger.info('Enqueuing %d job%s...' % (njobs, 's'[:njobs-1]))

        # submit to queue
        with Connection(Redis(redishost)):
            q = Queue(qname)

            # enqueue all but one
            if njobs > 1:
                for i in range(njobs-1):
                    state, segment = stateseg[i]
                    job = q.enqueue_call(func=rt.pipeline, args=(state, segment), depends_on=depends_on, timeout=6*3600, result_ttl=24*3600)
            else:
                job = depends_on

            # use second to last job as dependency for last job
            state, segment = stateseg[-1]
            lastjob = q.enqueue_call(func=rt.pipeline, args=(state, segment), depends_on=job, at_front=True, timeout=6*3600, result_ttl=24*3600)  # queued after others, but moved to front of queue

        logger.info('Jobs enqueued. Returning last job with id %s.' % lastjob.id)
        return lastjob
    else:
        logger.info('No jobs to enqueue')
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

def cleanup(workdir, fileroot, scans=[]):
    """ Cleanup up noise and cands files.
    Finds all segments in each scan and merges them into single cand/noise file per scan.
    """

    os.chdir(workdir)

    # merge cands/noise files per scan
    for scan in scans:
#try:
        pc.merge_segments(fileroot, scan, cleanup=True)
#        except:
#            logger.exception('')

def plot_summary(workdir, fileroot, scans, remove=[]):
    """ Make summary plots for cands/noise files with fileroot
    Uses only given scans.
    """

    os.chdir(workdir)

    try:
        pc.plot_summary(fileroot, scans, remove=remove)
        pc.plot_noise(fileroot, scans, remove=remove)
    except:
        logger.exception('')

    logger.info('Completed plotting for fileroot %s and scans %s' % (fileroot, str(scans)))

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

    logger.info('Pulsar plotting for pkllist:', pkllist)
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
                logger.info('No scans found for source %s' % source)
    elif intent:
        meta = sdmreader.read_metadata(filename, bdfdir=bdfdir)
        scans = filter(lambda sc: intent in meta[0][sc]['intent'], meta[0].keys())
#        scans = [sc for sc in meta[0].keys() if intent in meta[0][sc]['intent']]   # get all target fields
    else:
        logger.error('Must provide scans, sources, or intent.')
        raise BaseException

    return scans

def grouprange(start, size, step):
    arr = range(start,start+size)
    return [arr[ss:ss+step] for ss in range(0, len(arr), step)]

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
        logger.info('Copying %s into %s' % (fname, workdir))
        shutil.copytree(filename, newfileloc)  # copy file in
    else:
        logger.info('File %s already in %s. Using that one...' % (fname, workdir))
    filename = newfileloc

def check_spw(sdmfile, scan):
    """ Looks at relative freq of spw and duplicate spw_reffreq. 
    Returns 1 for permutable order with no duplicates and 0 otherwise (i.e., funny data)
    """

    d = rt.set_pipeline(sdmfile, scan, silent=True)

    dfreq = [d['spw_reffreq'][i+1] - d['spw_reffreq'][i] for i in range(len(d['spw_reffreq'])-1)]
    dfreqneg = [df for df in dfreq if df < 0]

    duplicates = list(set(d['spw_reffreq'])) != d['spw_reffreq']

    return len(dfreqneg) <= 1 and not duplicates

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
        logger.info('Looking for telcalfile in %s' % telcaldir2)
        telcalfile = [os.path.join(telcaldir2, ff) for ff in os.listdir(telcaldir2) if fname+'.GN' in ff]
        
        # if not in latest directory, walk through whole structure
        if not len(telcalfile):
            logger.info('No telcal in newest directory. Searching whole telcalfile tree.')
            telcalfile = [os.path.join(root, fname+'.GN') for root, dirs, files in os.walk(telcaldir) if fname+'.GN' in files]

        assert isinstance(telcalfile, list)

        # make into string (emtpy or otherwise)
        if len(telcalfile) == 1:
            telcalfile = telcalfile[0]
            logger.info('Found telcal file at %s' % telcalfile)
            break
        elif len(telcalfile) > 1:
            telcalfile = ''
            logger.info('Found multiple telcalfiles %s' % telcalfile)
        else:
            telcalfile = ''
            logger.info('No telcal file found in %s' % telcaldir)

        assert isinstance(telcalfile, str)

        # if waiting, but no file found, check timeout
        if timeout:
            if time.time() - time_filestart < timeout:  # don't break yet
                logger.info('Waiting for telcalfile...')
                time.sleep(2)
                continue
            else:   # reached timeout
                logger.info('Timeout waiting for telcalfile')
                break
        else:  # not waiting
            logger.info('Not waiting for telcalfile')
            break

    return telcalfile

def lookforfile(lookdir, subname, changesonly=False):
    """ Look for and return a file with subname in lookdir.
    changesonly means it will wait for changes. default looks only once.
    """

    logger.info('Looking for %s in %s.' % (subname, lookdir))

    filelist0 = os.listdir(os.path.abspath(lookdir))
    if changesonly:
        while 1:
            filelist = os.listdir(os.path.abspath(lookdir))
            newfiles = filter(lambda ff: ff not in filelist0, filelist)
            matchfiles = filter(lambda ff: subname in ff, newfiles)
            if len(matchfiles):
                break
            else:
                logger.info('.')
                filelist0 = filelist
                time.sleep(1)
    else:
        matchfiles = filter(lambda ff: subname in ff, filelist0)

    if len(matchfiles) == 0:
        logger.info('No file found.')
        fullname = ''
    elif len(matchfiles) == 1:
        fullname = os.path.join(lookdir, matchfiles[0])
    elif len(matchfiles) > 1:
        logger.info('More than one match!', matchfiles)
        fullname = os.path.join(lookdir, matchfiles[0])

    logger.info('Returning %s.' % fullname)
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
            logger.info('File %s not found.' % filename)
        except IOError:
            logger.info('File %s does not have Antenna.xml yet...' % filename)
            time.sleep(2)
            continue
        else:
            bdflocs = [sc[ss]['bdfstr'] for ss in sc]
            if not time_filestart:
                logger.info('File %s exists. Waiting for it to complete writing.' % filename)
                time_filestart = time.time()

        # if any bdfstr are not set, then file not finished
        if None in bdflocs:
            if time.time() - time_filestart < timeout:
                logger.info('bdfs not all written yet. Waiting...')
                time.sleep(2)
                continue
            else:
                logger.info('Timeout exceeded. Exiting...')
                break
        else:
            logger.info('All bdfs written. Continuing.')
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
        logger.info('Found existing cal xml and data. Moving in...')
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
                logger.info('Copying bdf in for calscan %d.' % calscan)
                shutil.copyfile(bdffile, bdffiledest)
            else:
                logger.info('bdf in for calscan %d already in place.' % calscan)

def sdmasorig(filename):
    """ Take sdm for calibration and restore it.
    keeps ASDMBinary around, just in case
    """

    shutil.move(os.path.join(filename, 'Main.xml'), os.path.join(filename, 'Main_cal.xml'))
    shutil.move(os.path.join(filename, 'Main_orig.xml'), os.path.join(filename, 'Main.xml'))
    shutil.move(os.path.join(filename, 'ASDMBinary'), os.path.join(filename, 'ASDMBinary_cal'))
