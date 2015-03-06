#!/usr/bin/env python2.7
#
# split job into nsegment pieces and queue all up with rq
# each job is independent and shares memory. one worker per node.

from rq import Queue, Connection
import os, argparse, string, logging

logging.basicConfig(filename='leanpipedt.log', level=logging.INFO)
#logging.config.dictConfig({'version': 1, 'disable_existing_loggers': False, 'formatters': {'standard': {'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'}, }, 'handlers': {'default': {'level':'INFO', 'class':'logging.StreamHandler', }, }, 'loggers': {'': {'handlers': ['default'], 'level': 'INFO', 'propagate': True}}})

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="MS filename with full path")
parser.add_argument("--scans", help="scans to search. MS value, not index.", default='-1')
parser.add_argument("--mode", help="'run', 'failed', 'clear'", default='run')
args = parser.parse_args(); filename = args.filename; scans = args.scans

# parameters of search **should be from argv**
workdir = string.join(filename.rstrip('/').split('/')[:-1], '/') + '/'
if workdir == '/':
    workdir = os.getcwd() + '/'
filename = filename.rstrip('/').split('/')[-1]
fileroot = filename.split('_s')[0]
gainfile= workdir + fileroot + '.g2'
bpfile= workdir + fileroot + '.b1'
threshold = 6.0    # 6.5sigma should produce 7.5 false+ for 512x512 imgs, 30 dms, and 23900 ints                                                                       
nints = 200*130  # max number of ints. should terminate when at end of data...
nskip = 300      # data reading parameters                                                                                       
iterint = 200    # helps if multiple of 2,4,8. must not allow much fringe rotation.                                              
spw = [0,1]
chans = range(6,122)+range(134,250)
filtershape = 'z'      # time filter                                                                                             
res = 58   # imaging parameters. set res=size=0 to define from uv coords                                                         
npix = 512
size = npix*res
searchtype = 'lowmem'
secondaryfilter = 'fullim'
dtarr = [1,2,4,8]   # integer to integrate in time for independent searches                                                      
dmarr = [0,19.2033,38.4033,57.6025,76.8036,96.0093,115.222,134.445,153.68,172.93,192.198,211.486,230.797,250.133,269.498,288.894,308.323,327.788,347.292,366.837,386.426,406.062,425.747,445.484,465.276,485.125,505.033,525.005,545.042,565.147,585.322,605.571,625.896,646.3,666.786,687.355,708.012,728.759,749.598,770.532,791.565,812.699,833.936,855.28,876.733,898.299,919.979,941.778,963.697,985.741,1007.91,1030.21,1052.64,1075.21,1097.92,1120.76,1143.76,1166.9,1190.19,1213.63,1237.23,1260.99,1284.92,1309.01,1333.27,1357.7,1382.31,1407.09,1432.06,1457.22,1482.56,1508.1,1533.83,1559.76,1585.89,1612.23,1638.77,1665.53,1692.51,1719.7,1747.12,1774.77,1802.64,1830.75,1859.1,1887.69,1916.53,1945.61,1974.95,2004.54,2034.39,2064.51,2094.9,2125.56,2156.49,2187.71,2219.21,2250.99,2283.07,2315.45,2348.13,2381.12,2414.41,2448.02,2481.94,2516.19,2550.77,2585.68,2620.92,2656.51,2692.44,2728.72,2765.36,2802.36,2839.72,2877.45,2915.55,2954.04,2992.91]
flagmode = 'standard'
nthreads = 15
excludeants = []
dmbin0 = 0
dmbin1 = 119
scans = [int(i) for i in args.scans.split(',')]
if scans[0] == -1:
    scans = [int(ss) for ss in filename.rstrip('.ms').split('_s')[1].split(',')]  # attempt to extract scans from filename

def main():
    import leanpipedt

    # queue jobs
    for scan in scans:
        scanind = scans.index(scan)
        candsfile = workdir + 'cands_' + fileroot + '_s' + str(scan) + '_dm' + str(dmbin0) + '-' + str(dmbin1) + '.pkl'
        if os.path.exists(candsfile):
            print '%s candidate file already exists. Stopping.' % candsfile
        else:
            print 'Adding scan %d of file %s to the queue' % (scan, filename)
            q.enqueue_call(func=leanpipedt.pipe_thread, args=(workdir+filename, nints, nskip, iterint, spw, chans, dmarr, dtarr, 0.5, 0.5, ['RR','LL'], scanind, 'data', size, res, threshold, threshold, filtershape, secondaryfilter, 1.5, searchtype, '', '', gainfile, bpfile, True, candsfile, flagmode, True, nthreads, 0, excludeants), timeout=24*3600, result_ttl=24*3600)

if __name__ == '__main__':
    # connect
    with Connection():

        if args.mode == 'run':
            q = Queue('low')
            main()

        elif args.mode == 'clear':
            q = Queue('high')
            q.empty()
            q = Queue('low')
            q.empty()
            q = Queue('failed')
            q.empty()

        elif args.mode == 'failed':
            q = Queue('failed')
            print 'Failed queue:'
            print q.jobs
            if len(q.jobs):
                print 'First in failed queue:'
                print q.jobs[0].exc_info
