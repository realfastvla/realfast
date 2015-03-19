#!/usr/bin/env python2.7
#
# split job into nsegment pieces and queue all up with rq
# each job is independent but shares file system. one worker per node.

from rq import Queue, Connection
import os, glob, time, argparse, pickle, string
import sdmreader
import rtpipe.RT as rt
import rtpipe.parsesdm as ps
import rtpipe.parsecands as pc
import rtpipe.calpipe as cp

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="filename with full path")
parser.add_argument("--scans", help="scans to search. comma-delimited integers.", default=0)
parser.add_argument("--sources", help="sources to search. comma-delimited source names (substring matched)", default='')
parser.add_argument("--mode", help="'read', 'search', 'calibrate'", default='read')
parser.add_argument("--paramfile", help="parameters for rtpipe using python-like syntax (custom parser for now)", default='')
parser.add_argument("--queue", help="Force queue priority ('high', 'low')", default='')
parser.add_argument("--fileroot", help="Root name for data products (used by calibrate for now)", default='')
args = parser.parse_args(); filename = args.filename; scans = args.scans; sources = args.sources; mode = args.mode; paramfile = args.paramfile; fileroot=args.fileroot

# get working directory and filename separately
workdir, filename = os.path.split(os.path.abspath(filename))

# if no scans defined, set by mode context
if scans != 0:
    scans = [int(i) for i in scans.split(',')]
else:
    meta = sdmreader.read_metadata(filename)

    # if source list provided, parse it then append all scans to single list
    if sources != '':
        sources = [i for i in sources.split(',')]
        scans = []
        for source in sources:
            sclist = [sc for sc in meta[0].keys() if source in meta[0][sc]['source']]
            if len(sclist):
                scans += sclist
            else:
                print 'No scans found for source %s' % source
    else:
        if mode == 'search':
            scans = [sc for sc in meta[0].keys() if 'TARGET' in meta[0][sc]['intent']]   # get all target fields
        else:
            scans = [sc for sc in meta[0].keys()]  # get all scans

def read():
    """ Simple parse and return metadata for pipeline for first scan
    """

    sc, sr = sdmreader.read_metadata(filename)
    print
    print 'Scans, Target names:'
    print [(ss, sc[ss]['source']) for ss in sc.keys()]
    print
    print 'Example pipeline for first scan:'
    state = rt.set_pipeline(os.path.join(workdir, filename), scans[0], paramfile=paramfile)

def search(depends_on=''):
    """ Search for transients in all target scans and segments
    """

    joblist = []
    # queue jobs
    for scan in scans:
        scanind = scans.index(scan)
        print 'Setting up pipeline for %s, scan %d' % (filename, scan)
        state = rt.set_pipeline(os.path.join(workdir, filename), scan, paramfile=paramfile, fileroot=fileroot)
        print 'Sending %d segments to queue' % (state['nsegments'])
        for segment in range(state['nsegments']):
            joblist.append(q.enqueue_call(func=rt.pipeline, args=(state, segment), timeout=24*3600, result_ttl=24*3600, depends_on=depends_on))
    return joblist

def calibrate():
    """ Run calibration pipeline
    """

    pipe = cp.pipe(os.path.join(workdir, filename), fileroot)
    job = q.enqueue_call(pipe.run, timeout=24*3600, result_ttl=24*3600)
    return job

def calimg(depends_on=''):
    """ Search of a small segment of data without dedispersion.
    Intended to test calibration quality.
    """

    timescale = 1.  # average to this timescale (sec)
    joblist = []
    for scan in scans:
        state = ps.get_metadata(os.path.join(workdir, filename), scan)
        read_downsample = int(timescale/state['inttime'])
        state = rt.set_pipeline(os.path.join(workdir, filename), scan, paramfile=paramfile, nthread=1, nsegments=0, gainfile=os.path.join(workdir, gainfile), bpfile=os.path.join(workdir, bpfile), dmarr=[0], dtarr=[1], timesub='', candsfile='', noisefile='', read_downsample=read_downsample, fileroot=fileroot)
        joblist.append(q.enqueue_call(func=rt.pipeline, args=(state, state['nsegments']/2), timeout=24*3600, result_ttl=24*3600, depends_on=depends_on))  # image middle segment
    return joblist

def watch():
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

def cleanup():
    """ Cleanup up noise and cands files.
    Finds all segments in each scan and merges them into single cand/noise file per scan.
    """

    # merge cands files
    for scan in scans:
        try:
            pkllist = glob.glob('cands_' + fileroot + '_sc' + str(scan) + 'seg*.pkl')
            pc.merge_pkl(pkllist, fileroot + '_sc' + str(scan))
        except AssertionError:
            print 'No cands files found for scan %d' % scan

        if os.path.exists('cands_' + fileroot + '_sc' + str(scan) + '.pkl'):
            for cc in pkllist:
                os.remove(cc)

        # merge noise files
        try:
            pkllist = glob.glob('noise_' + fileroot + '_sc' + str(scan) + 'seg*.pkl')
            pc.merge_pkl(pkllist, fileroot + '_sc' + str(scan))
        except AssertionError:
            print 'No noise files found for scan %d' % scan

        if os.path.exists('noise_' + fileroot + '_sc' + str(scan) + '.pkl'):
            for cc in pkllist:
                os.remove(cc)

def plot_cands():
    """
    Make summary plots.
    pkllist gives list of cand pkl files for visualization.
    default mode is to make cand and noise summary plots
    """

    pkllist = []
    for scan in scans:
        pkllist.append('cands_' + fileroot + '_sc' + str(scan) + '.pkl')
    pc.plot_cands(pkllist)
    
    pkllist = []
    for scan in scans:
        pkllist.append('noise_' + fileroot + '_sc' + str(scan) + '.pkl')
    pc.plot_noise(pkllist)

def plot_pulsar():
    """
    Assumes 3 or 4 input pulsar scans (centered then offset pointings).
    """

    pkllist = []
    for scan in scans:
        pkllist.append('cands_' + fileroot + '_sc' + str(scan) + '.pkl')

    pc.plot_psrrates(pkllist, outname='plot_' + fileroot + '_psrrates.png')


###############
# Job Control #
###############

if __name__ == '__main__':

    # define queue
    defaultqpriority = {'search': 'low', 'calibrate': 'high', 'calimg': 'high'}
    if mode in defaultqpriority.keys():
        if args.queue in ['high', 'low']:
            qpriority = args.queue
        else:
            qpriority = defaultqpriority[mode]

    # connect
    with Connection():
        if mode == 'read':
            read()

        elif mode == 'search':
            q = Queue(qpriority)
            search()

        elif mode == 'calibrate':
            q = Queue(qpriority)
            calibrate()
#            calimg()

        elif mode == 'all':
            q = Queue('high')
            watchjob = q.enqueue_call(func=watch, timeout=24*3600, result_ttl=24*3600)
            caljob = calibrate(depends_on=watchjob)
            q = Queue('low')
            searchjoblist = search(depends_on=caljob)
            q = Queue('high')
#            cleanjob = q.enqueue_call(func=cleanup, args=(joblist,), timeout=24*3600, result_ttl=24*3600, depends_on=searchjoblist)  # not yet possible in rq
#            plotjob = q.enqueue_call(func=plot_cands, args=(job,) timeout=24*3600, result_ttl=24*3600, depends_on=cleanjob)

        elif mode == 'calimg':
            q = Queue(qpriority)
            calimg()

        elif mode == 'cleanup':
#            q = Queue(qpriority)    # ultimately need this to be on queue and depende_on search
            cleanup()

        elif mode == 'plot_cands':
            plot_cands()

        elif mode == 'plot_pulsar':
            plot_pulsar()
