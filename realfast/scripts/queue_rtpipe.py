#!/usr/bin/env python2.7
#
# split job into nsegment pieces and queue all up with rq
# each job is independent but shares file system. one worker per node.

from rq import Queue, Connection
import os, argparse, time, shutil
import sdmreader, queue_monitor
from realfast import rtutils

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="filename with full path")
parser.add_argument("--mode", help="'read', 'search', 'calibrate', 'all'", default='read')
parser.add_argument("--paramfile", help="parameters for rtpipe using python-like syntax (custom parser for now)", default='')
parser.add_argument("--fileroot", help="Root name for data products (used by calibrate for now)", default='')
parser.add_argument("--sources", help="sources to search. comma-delimited source names (substring matched)", default='')
parser.add_argument("--scans", help="scans to search. comma-delimited integers.", default='')
parser.add_argument("--queue", help="Force queue priority ('high', 'low')", default='')
parser.add_argument("--candnum", help="Candidate number to plot", default=-1)
parser.add_argument("--remove", help="List of times to remove plot_summary visualizations", nargs='+', type=float, default=[])
args = parser.parse_args(); filename = args.filename; scans = args.scans; sources = args.sources; mode = args.mode; paramfile = args.paramfile; fileroot=args.fileroot; candnum = int(args.candnum); remove = args.remove

# Define names, paths
redishost = os.uname()[1]
filename = os.path.abspath(filename)
if paramfile:
    paramfile = os.path.abspath(paramfile)

bdfdir = '/lustre/evla/wcbe/data/realfast' # '/lustre/evla/wcbe/data/bunker'
sdmdir = '/home/mchammer/evla/mcaf/workspace'
telcaldir = '/home/mchammer/evladata/telcal'  # then yyyy/mm
workdir = os.getcwd()  # or set to '/users/claw/lustrecbe/'?

# Job Control
if __name__ == '__main__':
    defaultqpriority = {}  # option for function-specific queuing (not yet supported by rq, though)
    if mode in defaultqpriority.keys():
        if args.queue:
            qpriority = args.queue
        else:
            qpriority = defaultqpriority[mode]
    else:
        qpriority = 'default'

    # if look/wait in mode, don't get scans yet
    if not any(filter(lambda mm: mm in mode, ['look', 'wait'])):
        scans = rtutils.getscans(filename, sources=sources, scans=scans, intent='TARGET')  # default cleans up target scans

    # connect
    with Connection():
        if mode == 'read':
            q = Queue(qpriority, async=False)  # run locally
            readjob = q.enqueue_call(func=rtutils.read, args=(filename, paramfile, fileroot, bdfdir), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'search':
            q = Queue(qpriority)
            lastjob = rtutils.search(qpriority, filename, paramfile, fileroot, scans=scans)  # default TARGET intent

        elif mode == 'rtsearch':
            q = Queue(qpriority)
            lastjob = rtutils.rtsearch(qpriority, filename, workdir, paramfile, fileroot, telcaldir, scans=scans)  # default TARGET intent
            q.enqueue_call(func=queue_monitor.addjob, args=(lastjob.id,))

        elif mode == 'calibrate':
            q = Queue(qpriority)
            caljob = q.enqueue_call(func=rtutils.calibrate, args=(filename, fileroot), timeout=24*3600, result_ttl=24*3600)
            
        elif mode == 'cleanup':
            q = Queue(qpriority, async=False)
            cleanjob = q.enqueue_call(func=rtutils.cleanup, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)    # default TARGET intent

        elif mode == 'plot_summary':
            q = Queue(qpriority, async=False)
            plotjob = q.enqueue_call(func=rtutils.plot_summary, args=(workdir, fileroot, scans, remove), timeout=24*3600, result_ttl=24*3600)   # default TARGET intent

        elif mode == 'show_cands':
            q = Queue(qpriority, async=False)
            plotjob = q.enqueue_call(func=rtutils.plot_cand, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)   # default TARGET intent

        elif mode == 'plot_cand':
            q = Queue(qpriority)
            plotjob = q.enqueue_call(func=rtutils.plot_cand, args=(workdir, fileroot, scans, candnum), timeout=24*3600, result_ttl=24*3600)    # default TARGET intent 

        elif mode == 'plot_pulsar':
            q = Queue(qpriority, async=False)    # ultimately need this to be on queue and depende_on search
            plotjob = q.enqueue_call(func=rtutils.plot_pulsar, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)    # default TARGET intent

        elif mode == 'lookalldaemon':
            # this mode looks for file that includes filename, checks that it is completed sdm, then runs 'all'
            subname = os.path.split(filename)[1]
            rtutils.lookalldaemon(sdmdir, subname, workdir, paramfile, fileroot, qpriority, redishost, newonly=False)

        elif mode == 'all':
            q = Queue('default')
#            waitjob = q.enqueue_call(func=rtutils.lookforfile, args=(sdmdir, filename, True), timeout=24*3600, result_ttl=24*3600)            # watch function not ready, since it prematurely triggers on data while being written
            caljob = q.enqueue_call(func=rtutils.calibrate, args=(filename, fileroot), timeout=24*3600, result_ttl=24*3600)   # can be set to enqueue when data arrives
            lastsearchjob = rtutils.search(q.name, filename, paramfile, fileroot, scans=scans, redishost=redishost, depends_on=caljob)
            cleanjob = q.enqueue_call(func=rtutils.cleanup, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=lastsearchjob)  # enqueued when joblist finishes
            plotjob = q.enqueue_call(func=rtutils.plot_summary, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=cleanjob)   # enqueued when cleanup finished
        else:
            print 'mode %s not recognized.' % mode
