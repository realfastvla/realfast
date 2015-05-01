#!/usr/bin/env python2.7
#
# split job into nsegment pieces and queue all up with rq
# each job is independent but shares file system. one worker per node.

from rq import Queue, Connection
import os, argparse, time, shutil, subprocess, glob
import sdmreader
import queue_funcs as qf

parser = argparse.ArgumentParser()
parser.add_argument("filename", help="filename with full path")
parser.add_argument("--mode", help="'read', 'search', 'calibrate', 'all'", default='read')
parser.add_argument("--paramfile", help="parameters for rtpipe using python-like syntax (custom parser for now)", default='')
parser.add_argument("--fileroot", help="Root name for data products (used by calibrate for now)", default='')
parser.add_argument("--sources", help="sources to search. comma-delimited source names (substring matched)", default='')
parser.add_argument("--scans", help="scans to search. comma-delimited integers.", default='')
parser.add_argument("--queue", help="Force queue priority ('high', 'low')", default='')
parser.add_argument("--candnum", help="Candidate number to plot", default=-1)
args = parser.parse_args(); filename = args.filename; scans = args.scans; sources = args.sources; mode = args.mode; paramfile = args.paramfile; fileroot=args.fileroot; candnum = int(args.candnum)

# Define names, paths
redishost = os.uname()[1]
filename = os.path.abspath(filename)
paramfile = os.path.abspath(paramfile)
bdfdir = '/lustre/evla/wcbe/data/bunker'
sdmdir = '/home/mchammer/evla/mcaf/workspace/'
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
        scans = qf.getscans(filename, sources=sources, scans=scans, intent='TARGET')  # default cleans up target scans

    # connect
    with Connection():
        if mode == 'read':
            q = Queue(qpriority, async=False)  # run locally
            readjob = q.enqueue_call(func=qf.read, args=(filename, paramfile), timeout=24*3600, result_ttl=24*3600)

        elif mode == 'looksearch':
            q = Queue(qpriority)
            filejob = q.enqueue_call(func=qf.lookforfile, args=(sdmdir, filename), timeout=24*3600, result_ttl=24*3600)
            searchjobids = qf.search(qpriority, filename, paramfile, fileroot, scans=scans, depends_on=filejob)  # default TARGET intent

        elif mode == 'search':
            searchjobids = qf.search(qpriority, filename, paramfile, fileroot, scans=scans)  # default TARGET intent

        elif mode == 'calibrate':
            q = Queue(qpriority)
            caljob = q.enqueue_call(func=qf.calibrate, args=(filename, fileroot), timeout=24*3600, result_ttl=24*3600)
            
        elif mode == 'cleanup':
            q = Queue(qpriority, async=False)
            cleanjob = q.enqueue_call(func=qf.cleanup, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)    # default TARGET intent

        elif mode == 'plot_summary':
            q = Queue(qpriority, async=False)
            plotjob = q.enqueue_call(func=qf.plot_summary, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)   # default TARGET intent

        elif mode == 'show_cands':
            q = Queue(qpriority, async=False)
            plotjob = q.enqueue_call(func=qf.plot_cand, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)   # default TARGET intent

        elif mode == 'plot_cand':
            q = Queue(qpriority)
            plotjob = q.enqueue_call(func=qf.plot_cand, args=(workdir, fileroot, scans, candnum), timeout=24*3600, result_ttl=24*3600)    # default TARGET intent 

        elif mode == 'plot_pulsar':
            q = Queue(qpriority, async=False)    # ultimately need this to be on queue and depende_on search
            plotjob = q.enqueue_call(func=qf.plot_pulsar, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600)    # default TARGET intent

        elif mode == 'looktest':
            # this mode looks for file that includes filename, checks that it is completed sdm, then runs 'all'

            q = Queue(qpriority, async=False)  # this will block
            subname = os.path.split(filename)[1]
            filejob = q.enqueue_call(func=qf.lookforfile, args=(sdmdir, subname), timeout=24*3600, result_ttl=24*3600)
            filename = filejob.result
            sdmjob = q.enqueue_call(func=qf.waitforsdm, args=(filename,), timeout=24*3600, result_ttl=24*3600)

            # make symlink to working area
            newfileloc = os.path.join(workdir, os.path.split(filename)[1])
#            os.symlink(filename, newfileloc)
            shutil.copytree(filename, newfileloc)  # copy file in
            calscanlist = qf.getscans(newfileloc, intent='CALI')   # get calibration scans
            calscans = ','.join([str(sc) for sc in calscanlist])   # put into CASA-like selection string
            shutil.copyfile(newfileloc + '/Main.xml', newfileloc + '/Main_orig.xml')   # put new Main.xml in
            subprocess.call(['choose_SDM_scans.pl', newfileloc, os.path.join(workdir, 'Main.xml'), calscans])  #  modify Main.xml
# not copying?
            shutil.move(os.path.join(workdir, 'Main.xml'), newfileloc + '/Main.xml')   # put new Main.xml in
            os.makedirs(os.path.join(newfileloc, 'ASDMBinary'))
            sc,sr = sdmreader.read_metadata(newfileloc)
            for calscan in calscanlist:
                print 'Copying bdf in for calscan %d' % calscan
                bdfnum = sc[calscan]['bdfstr'].split('/')[-1]
                bdffile = glob.glob(os.path.join(bdfdir, bdfnum))[0]
                shutil.copyfile(bdffile, os.path.join(newfileloc, 'ASDMBinary', os.path.split(bdffile)[1]))
            filename = newfileloc

            # start jobs
            q = Queue(qpriority)
            caljob = q.enqueue_call(func=qf.calibrate, args=(filename, fileroot), timeout=24*3600, result_ttl=24*3600)   # can be set to enqueue when data arrives

        elif mode == 'lookall':
            # this mode looks for file that includes filename, checks that it is completed sdm, then runs 'all'

            q = Queue(qpriority, async=False)  # this will block
            subname = os.path.split(filename)[1]
            filejob = q.enqueue_call(func=qf.lookforfile, args=(sdmdir, subname), timeout=24*3600, result_ttl=24*3600)
            filename = filejob.result
            sdmjob = q.enqueue_call(func=qf.waitforsdm, args=(filename,), timeout=24*3600, result_ttl=24*3600)

            # make symlink to working area
            newfileloc = os.path.join(workdir, os.path.split(filename)[1])
#            os.symlink(filename, newfileloc)
            shutil.copytree(filename, newfileloc)  # copy file in
            calscanlist = qf.getscans(newfileloc, intent='CALI')   # get calibration scans
            calscans = ','.join([str(sc) for sc in calscanlist])   # put into CASA-like selection string
            shutil.copyfile(newfileloc + '/Main.xml', newfileloc + '/Main_orig.xml')   # put new Main.xml in
            subprocess.call(['choose_SDM_scans.pl', newfileloc, os.path.join(workdir, 'Main.xml'), calscans])  #  modify Main.xml
# not copying?
            shutil.move(os.path.join(workdir, 'Main.xml'), newfileloc + '/Main.xml')   # put new Main.xml in
            os.makedirs(os.path.join(newfileloc, 'ASDMBinary'))
            sc,sr = sdmreader.read_metadata(newfileloc)
            for calscan in calscanlist:
                print 'Copying bdf in for calscan %d' % calscan
                bdfnum = sc[calscan]['bdfstr'].split('/')[-1]
                bdffile = glob.glob(os.path.join(bdfdir, bdfnum))[0]
                shutil.copyfile(bdffile, os.path.join(newfileloc, 'ASDMBinary'))
            filename = newfileloc

            # start jobs
            q = Queue(qpriority)
            caljob = q.enqueue_call(func=qf.calibrate, args=(filename, fileroot), timeout=24*3600, result_ttl=24*3600)   # can be set to enqueue when data arrives

            shutil.move(filename + '/Main_orig.xml', filename + '/Main.xml')   # put orig Main.xml in
            scans = qf.getscans(filename, sources=sources, scans=scans, intent='TARGET')  # default cleans up target scans
#            lastsearchjob = qf.search(q.name, filename, paramfile, fileroot, scans=scans, redishost=redishost, depends_on=caljob)
            lastsearchjob = qf.search(q.name, filename, paramfile, fileroot, scans=scans, redishost=redishost)
            cleanjob = q.enqueue_call(func=qf.cleanup, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=lastsearchjob)  # enqueued when joblist finishes
            plotjob = q.enqueue_call(func=qf.plot_summary, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=cleanjob)   # enqueued when cleanup finished

        elif mode == 'all':
            q = Queue('default')
#            waitjob = q.enqueue_call(func=qf.lookforfile, args=(sdmdir, filename, True), timeout=24*3600, result_ttl=24*3600)            # watch function not ready, since it prematurely triggers on data while being written
            caljob = q.enqueue_call(func=qf.calibrate, args=(filename, fileroot), timeout=24*3600, result_ttl=24*3600)   # can be set to enqueue when data arrives
            lastsearchjob = qf.search(q.name, filename, paramfile, fileroot, scans=scans, redishost=redishost, depends_on=caljob)
            cleanjob = q.enqueue_call(func=qf.cleanup, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=lastsearchjob)  # enqueued when joblist finishes
            plotjob = q.enqueue_call(func=qf.plot_summary, args=(workdir, fileroot, scans), timeout=24*3600, result_ttl=24*3600, depends_on=cleanjob)   # enqueued when cleanup finished
        else:
            print 'mode %s not recognized.' % mode
