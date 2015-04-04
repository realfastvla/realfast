#!/usr/bin/env python2.7
#
# split job into nsegment pieces and queue all up with rq
# each job is independent but shares file system. one worker per node.

from rq import Queue, Connection
import os, argparse, time
import sdmreader
import queue_funcs as qf

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="filename with full path")
parser.add_argument("--scans", help="scans to search. comma-delimited integers.", default=0)
parser.add_argument("--sources", help="sources to search. comma-delimited source names (substring matched)", default='')
parser.add_argument("--mode", help="'read', 'search', 'calibrate', 'all'", default='read')
parser.add_argument("--paramfile", help="parameters for rtpipe using python-like syntax (custom parser for now)", default='')
parser.add_argument("--queue", help="Force queue priority ('high', 'low')", default='')
parser.add_argument("--fileroot", help="Root name for data products (used by calibrate for now)", default='')
parser.add_argument("--candnum", help="Candidate number to plot", default=-1)
args = parser.parse_args(); filename = args.filename; scans = args.scans; sources = args.sources; mode = args.mode; paramfile = args.paramfile; fileroot=args.fileroot; candnum = int(args.candnum)

redishost = os.uname()[1]

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
        if (mode == 'search') or (mode == 'all'):
            scans = [sc for sc in meta[0].keys() if 'TARGET' in meta[0][sc]['intent']]   # get all target fields
        else:
            scans = [sc for sc in meta[0].keys()]  # get all scans

###############
# Job Control #
###############

if __name__ == '__main__':

    # define queue
#    defaultqpriority = {'read': 'high','search': 'low', 'calibrate': 'high', 'calimg': 'high', 'cleanup': 'high', 'plot_cands': 'high', 'plot_pulsar': 'high'}
    defaultqpriority = {}
    if mode in defaultqpriority.keys():
        if args.queue:
            qpriority = args.queue
        else:
            qpriority = defaultqpriority[mode]
    else:
        qpriority = 'default'

    # connect
    with Connection():
        if mode == 'read':
            q = Queue(qpriority, async=False)  # run locally
            readjob = q.enqueue_call(func=qf.read, args=(workdir, filename, paramfile), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'search':
            searchjobids = qf.search(qpriority, workdir, filename, paramfile, fileroot, scans)

        elif mode == 'calibrate':
            q = Queue(qpriority)
            caljob = q.enqueue_call(func=qf.calibrate, args=(workdir, filename, fileroot), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'calimg':
            q = Queue(qpriority)
            
        elif mode == 'cleanup':
            q = Queue(qpriority)
            cleanjob = q.enqueue_call(func=qf.cleanup, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'plot_summary':
            q = Queue(qpriority)
            plotjob = q.enqueue_call(func=qf.plot_summary, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'show_cands':
            q = Queue(qpriority, async=False)
            plotjob = q.enqueue_call(func=qf.plot_cand, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'plot_cand':
            q = Queue(qpriority)
            plotjob = q.enqueue_call(func=qf.plot_cand, args=(workdir, fileroot, scans, candnum), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'plot_pulsar':
            q = Queue(qpriority)    # ultimately need this to be on queue and depende_on search
            plotjob = q.enqueue_call(func=qf.plot_pulsar, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'all':
            q = Queue('default')
#            watchjob = q.enqueue_call(func=qf.watch, args=(workdir, filename), timeout=24*3600, result_ttl=24*3600)            # watch function not ready, since it prematurely triggers on data while being written
            caljob = q.enqueue_call(func=qf.calibrate, args=(workdir, filename, fileroot), timeout=24*3600, result_ttl=24*3600)   # can be set to enqueue when data arrives
            lastsearchjob = qf.search(q.name, workdir, filename, paramfile, fileroot, scans, redishost, caljob)
            cleanjob = q.enqueue_call(func=qf.cleanup, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=lastsearchjob)  # enqueued when joblist finishes
            plotjob = q.enqueue_call(func=qf.plot_summary, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=cleanjob)   # enqueued when cleanup finished
