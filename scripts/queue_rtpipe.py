#!/usr/bin/env python2.7
#
# split job into nsegment pieces and queue all up with rq
# each job is independent but shares file system. one worker per node.

import uuid # workaround for cbe crash
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import os, argparse, time, shutil
from realfast import rtutils

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="filename with full path")
parser.add_argument("--mode", help="'read', 'search', 'calibrate', 'all'", default='read')
parser.add_argument("--paramfile", help="parameters for rtpipe using python-like syntax (custom parser for now)", default='')
parser.add_argument("--fileroot", help="Root name for data products (used by calibrate for now)", default='')
parser.add_argument("--sources", help="sources to search. comma-delimited source names (substring matched)", default='')
parser.add_argument("--scans", help="scans to search. comma-delimited integers.", default='')
parser.add_argument("--intent", help="Intent filter for getting scans", default='TARGET')
parser.add_argument("--queue", help="Force queue priority ('high', 'low')", default='')
parser.add_argument("--candnum", help="Candidate number to plot", default=-1)
parser.add_argument("--remove", help="List of times to remove plot_summary visualizations", nargs='+', type=float, default=[])
parser.add_argument("--snrmin", help="Min SNR to include in plot_summary", default=-999)
parser.add_argument("--snrmax", help="Max SNR to include in plot_summary", default=999)
args = parser.parse_args(); filename = args.filename; sources = args.sources; mode = args.mode; paramfile = args.paramfile; fileroot=args.fileroot; candnum = int(args.candnum); remove = args.remove; snrmin = float(args.snrmin); snrmax = float(args.snrmax); intent = args.intent

scans = rtutils.getscans(filename, scans=args.scans, sources=args.sources, intent=intent)

# Define names, paths
filename = os.path.abspath(filename)
if paramfile:
    paramfile = os.path.abspath(paramfile)
if not fileroot: fileroot = os.path.basename(filename)

telcaldir = '/home/mchammer/evladata/telcal'  # then yyyy/mm
workdir = os.getcwd()  # or set to '/users/claw/lustrecbe/'?

# Job Control
if __name__ == '__main__':
    defaultqpriority = {'plot_cand': 'default'}  # option for function-specific queuing (not yet supported by rq, though)
    if mode in defaultqpriority.keys():
        if args.queue:
            qpriority = args.queue
        else:
            qpriority = defaultqpriority[mode]
    else:
        qpriority = 'default'

    # connect
    if mode == 'read':
        rtutils.read(filename, paramfile, fileroot)

    elif mode == 'search':
        lastjob = rtutils.search(qpriority, filename, paramfile, fileroot, scans=scans)  # default TARGET intent

    elif mode == 'rtsearch':
        """ Real-time search on cbe. First copies sdm into workdir, then looks for telcalfile (optionally waiting with timeout), then queues jobs.
        """

        import queue_monitor

        # copy data into place
        rtutils.rsyncsdm(filename, workdir)
        filename = os.path.join(workdir, os.path.basename(filename))   # new full-path filename

        assert 'mchammer' not in filename  # be sure we are not working with pure version

        # find telcalfile (use timeout to wait for it to be written)
        telcalfile = rtutils.gettelcalfile(telcaldir, filename, timeout=60)

        # submit search job and add tail job to monitoring queue
        if telcalfile:
            lastjob = rtutils.search(qpriority, filename, paramfile, fileroot, scans, telcalfile=telcalfile, redishost=redishost)
            rtutils.addjob(lastjob.id)
        else:
            logger.info('No calibration available. No job submitted.')

    elif mode == 'calibrate':
        from rq import Queue
        from redis import Redis

        q = Queue(qpriority, connection=Redis())
        caljob = q.enqueue_call(func=rtutils.calibrate, args=(filename, fileroot), timeout=7*24*3600, result_ttl=7*24*3600)
            
    elif mode == 'cleanup':
        # pack all the segments up into one pkl per scan
        rtutils.cleanup(workdir, fileroot, scans)

    elif mode == 'plot_summary':
        rtutils.plot_summary(workdir, fileroot, scans, remove, snrmin=snrmin, snrmax=snrmax)

    elif mode == 'show_cands':
        rtutils.plot_cand(workdir, fileroot, scans)

    elif mode == 'plot_cand':
        from rq import Queue
        from redis import Redis
        
        q = Queue(qpriority, connection=Redis())
        plotjob = q.enqueue_call(func=rtutils.plot_cand, args=(workdir, fileroot, scans, candnum), timeout=7*24*3600, result_ttl=7*24*3600)    # default TARGET intent 

    elif mode == 'plot_pulsar':
        rtutils.plot_pulsar(workdir, fileroot, scans)

    else:
        logger.info('mode %s not recognized.' % mode)

