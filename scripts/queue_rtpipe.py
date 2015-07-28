#!/usr/bin/env python2.7
#
# split job into nsegment pieces and queue all up with rq
# each job is independent but shares file system. one worker per node.

from rq import Queue, Connection
import os, argparse, time, shutil
import sdmreader, queue_monitor
from realfast import rtutils, queue_monitor

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
args = parser.parse_args(); filename = args.filename; scans = [int(sc) for sc in args.scans.split(',')]; sources = args.sources; mode = args.mode; paramfile = args.paramfile; fileroot=args.fileroot; candnum = int(args.candnum); remove = args.remove

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

    # connect
    with Connection():
        if mode == 'read':
            q = Queue(qpriority, async=False)  # run locally
            readjob = q.enqueue_call(func=rtutils.read, args=(filename, paramfile, fileroot, bdfdir), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'search':
            q = Queue(qpriority)
            lastjob = rtutils.search(qpriority, filename, paramfile, fileroot, scans=scans)  # default TARGET intent

        elif mode == 'rtsearch':
            """ Real-time search on cbe. First copies sdm into workdir, then looks for telcalfile (optionally waiting with timeout), then queues jobs.
            """

            # copy data into place
            rtutils.rsyncsdm(filename, workdir)
            filename = os.path.join(workdir, os.path.basename(filename))   # new full-path filename

            assert 'mchammer' not in filename  # be sure we are not working with pure version

            # find telcalfile (use timeout to wait for it to be written)
            telcalfile = rtutils.gettelcalfile(telcaldir, filename, timeout=60)

            # submit search job and add tail job to monitoring queue
            if telcalfile:
                lastjob = rtutils.search(qpriority, filename, paramfile, fileroot, scans, telcalfile=telcalfile, redishost='localhost')
                queue_monitor.addjob(lastjob.id)
            else:
                print 'No calibration available. No job submitted.'

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

        else:
            print 'mode %s not recognized.' % mode
