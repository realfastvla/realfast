#!/usr/bin/env python2.7
#
# split job into nsegment pieces and queue all up with rq
# each job is independent but shares file system. one worker per node.

from rq import Queue, Connection
import os, glob, time, argparse, pickle, string

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="filename with full path")
parser.add_argument("--scans", help="scans to search. comma-delimited integers.", default=0)
parser.add_argument("--sources", help="sources to search. comma-delimited source names (substring matched)", default='')
parser.add_argument("--mode", help="'read', 'search', 'calibrate'", default='read')
parser.add_argument("--queue", help="Force queue priority ('high', 'low')", default='')
args = parser.parse_args(); filename = args.filename; scans = args.scans; sources = args.sources; mode = args.mode

# split path from filename
workdir = string.join(filename.rstrip('/').split('/')[:-1], '/') + '/'
if workdir == '/':
    workdir = os.getcwd() + '/'
filename = filename.rstrip('/').split('/')[-1]

# parameters of search (or could be done as argument?)
os.chdir(workdir)
try:
    from rtparams import *   # fill namespace with processing params
except ImportError:
    print 'No rtparams.py found in %s' % os.getcwd()

# scans to use depends on context
if scans != 0:
    scans = [int(i) for i in scans.split(',')]
else:
    import sdmreader
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
        elif mode == 'calibrate':
            scans = [sc for sc in meta[0].keys() if 'CAL' in meta[0][sc]['intent']]   # get all cal fields
        else:
            scans = [sc for sc in meta[0].keys()]  # get all scans

def read():
    """ Simple parse and return metadata for pipeline for first scan
    """
    import realtime.RT as rt
    import realtime.parsesdm as ps

    sc, sr = sdmreader.read_metadata(filename)
    print
    print 'Scans, Target names:'
    print [(ss, sc[ss]['source']) for ss in sc.keys()]
    print
    print 'Example pipeline for first scan:'
    state = ps.get_metadata(workdir + filename, scans[0], chans=chans, spw=spw)    
    rt.set_pipeline(state, nthread=nthread, nsegments=nsegments, gainfile=workdir + gainfile, bpfile=workdir + bpfile, dmarr=dmarr, dtarr=dtarr, timesub=timesub, candsfile=workdir + candsfile, noisefile=workdir + noisefile, sigma_image1=sigma_image1, flagmode=flagmode, flagantsol=flagantsol, searchtype=searchtype, uvres=uvres, npix=npix)

def search():
    """ Search for transients in all target scans and segments
    """
    import realtime.RT as rt
    import realtime.parsesdm as ps

    # queue jobs
    for scan in scans:
        scanind = scans.index(scan)
        print 'Getting metadata for %s, scan %d' % (filename, scan)
        state = ps.get_metadata(workdir + filename, scan, chans=chans, spw=spw)
        rt.set_pipeline(state, nthread=nthread, nsegments=nsegments, gainfile=workdir + gainfile, bpfile=workdir + bpfile, dmarr=dmarr, dtarr=dtarr, timesub=timesub, candsfile=workdir + candsfile, noisefile=workdir + noisefile, sigma_image1=sigma_image1, flagmode=flagmode, flagantsol=flagantsol, searchtype=searchtype, uvres=uvres, npix=npix)
        print 'Sending %d segments to queue' % (state['nsegments'])
        for segment in range(state['nsegments']):
            q.enqueue_call(func=rt.pipeline, args=(state, segment), timeout=24*3600, result_ttl=24*3600)

def calibrate():
    """ Run calibration pipeline
    """
    #flags = ["mode='unflag'", "mode='shadow'", "mode='clip' clipzeros=True", "mode='rflag' freqdevscale=4 timedevscale=5", "mode='extend' growaround=True growtime=60 growfreq=40 extendpols=True", "mode='quack' quackinterval=20", "mode='summary'"]
    #"mode='manual' antenna='ea11,ea19'", 

    import calpipe
    cp = calpipe.pipe(workdir + filename)
    cp.run()

def calimg():
    """ Search of a small segment of data without dedispersion.
    Intended to test calibration quality.
    """

    import realtime.RT as rt
    import realtime.parsesdm as ps

    timescale = 1.  # average to this timescale (sec)

    for scan in scans:
        state = ps.get_metadata(workdir + filename, scan, chans=chans, spw=spw)
        read_downsample = int(timescale/state['inttime'])
        rt.set_pipeline(state, nthread=1, nsegments=0, gainfile=workdir + gainfile, bpfile=workdir + bpfile, dmarr=[0], dtarr=[1], timesub='', candsfile='', noisefile='', sigma_image1=sigma_image1, flagmode=flagmode, flagantsol=flagantsol, searchtype=searchtype, uvres=uvres, npix=npix, read_downsample=read_downsample)
        q.enqueue_call(func=rt.pipeline, args=(state, state['nsegments']/2), timeout=24*3600, result_ttl=24*3600)  # image middle segment

def cleanup(fileroot, scans):
    """ Cleanup up noise and cands files.
    Finds all segments in each scan and merges them into single cand/noise file per scan.
    """

    import realtime.parsecands as pc

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

def plot_cands(fileroot, scans, candsfile, noisefile):
    """
    Make summary plots.
    pkllist gives list of cand pkl files for visualization.
    default mode is to make cand and noise summary plots
    """

    import realtime.parsecands as pc

    pkllist = []
    for scan in scans:
        pkllist.append('cands_' + fileroot + '_sc' + str(scan) + '.pkl')
    pc.plot_cands(pkllist)
    
    pkllist = []
    for scan in scans:
        pkllist.append('noise_' + fileroot + '_sc' + str(scan) + '.pkl')
    pc.plot_noise(pkllist)

def plot_pulsar(fileroot, scans):
    """
    Assumes 3 or 4 input pulsar scans (centered then offset pointings).
    """

    import realtime.parsecands as pc

    pkllist = []
    for scan in scans:
        pkllist.append('cands_' + fileroot + '_sc' + str(scan) + '.pkl')

    pc.plot_psrrates(pkllist, outname='plot_' + fileroot + '_psrrates.png')


###########
# Job Control #
###########

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
            calimg()

        elif mode == 'calimg':
            q = Queue(qpriority)
            calimg()

        elif mode == 'cleanup':
            cleanup(fileroot, scans)

        elif mode == 'plot_cands':
            plot_cands(fileroot, scans, candsfile, noisefile)

        elif mode == 'plot_pulsar':
            plot_pulsar(fileroot, scans)
