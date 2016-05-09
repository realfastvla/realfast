import uuid # workaround for cbe crash
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import click, os
from realfast import rtutils
from rq import Queue
from redis import Redis
from queue_monitor import movetoarchive
import rtpipe.parsecands as pc

# set up  
conn0 = Redis(db=0)
conn = Redis(db=1)   # db for tracking ids of tail jobs
trackercount = 2000  # number of tracking jobs (one per scan in db=1) to monitor 

@click.command()
@click.argument("filename")
@click.option("--mode", help="Options include: search, cleanup", default='search')
@click.option("--paramfile", help="parameters for rtpipe using python-like syntax (custom parser for now)", default='')
@click.option("--fileroot", help="Root name for data products (used by calibrate for now)", default='')
@click.option("--sources", help="sources to search. comma-delimited source names (substring matched)", default='')
@click.option("--scans", help="scans to search. comma-delimited integers.", default='')
@click.option("--intent", help="Intent filter for getting scans", default='TARGET')
@click.option("--snrmin", help="Min SNR to include in plot_summary", default=0.)
@click.option("--snrmax", help="Max SNR to include in plot_summary", default=999.)
@click.option("--candnum", help="Candidate number to plot", default=-1)
def rtpipe(filename, mode, paramfile, fileroot, sources, scans, intent, snrmin, snrmax, candnum):
    """ Function for command-line access to queue_rtpipe
    """

    # set up
    workdir = os.getcwd()
    qpriority = 'default'
    scans = rtutils.getscans(filename, scans=scans, sources=sources, intent=intent)
    filename = os.path.abspath(filename)
    if paramfile:
        paramfile = os.path.abspath(paramfile)
    if not fileroot: fileroot = os.path.basename(filename)

    # select by mode
    if mode == 'search':
        lastjob = rtutils.search(qpriority, filename, paramfile, fileroot, scans=scans)  # default TARGET intent

    elif mode == 'cleanup':
        pc.cleanup(workdir, fileroot, scans)

    elif mode == 'plot_cand':
        rtutils.plot_cand(workdir, fileroot, scans, candnum)

    elif mode == 'plot_pulsar':
        rtutils.plot_pulsar(workdir, fileroot, scans)

    else:
        logger.info('mode %s not recognized.' % mode)

@click.command()
def status():
    """ Quick dump of trace for all failed jobs
    """

    for qname in ['default', 'slow', 'failed']:
        q = Queue(qname, connection=conn0)
        logger.info('Jobs in queue %s:' % qname)
        for job in q.jobs:
            if isinstance(job.args[0], dict):
                details = (job.args[0]['filename'], job.args[0]['scan'])
            else:
                details = job.args
                logger.info('job %s, args: %s' % (job.id, str(details)))

    jobids = conn.scan(cursor=0, count=trackercount)[1]
    logger.info('Jobs in tracking queue:')
    q = Queue('default', connection=conn0)
    for jobid in jobids:
        job = q.fetch_job(jobid)
        try:
            logger.info('Job %s: filename %s, scan %d, segments, %s' % (job.id, job.args[0]['filename'], job.args[0]['scan'], str(job.args[1])))
        except:
            logger.info('Job %s: %s' % (job.id, job.args))

@click.command()
def failed():
    """ Quick dump of trace for all failed jobs
    """

    q = Queue('failed', connection=conn0)
    for i in range(len(q.jobs)):
        job = q.jobs[i]
        try:
            logger.info('Failed job %s: filename %s, scan %d, segments, %s' % (job.id, job.args[0]['filename'], job.args[0]['scan'], str(job.args[1])))
        except:
            logger.info('Failed job %s: %s' % (job.id, job.args))
        logger.info('%s' % job.exc_info)

@click.command()
def requeue():
    """ Take jobs from failed queue and add them to default queue
    """

    qf = Queue('failed', connection=conn0)
    q = Queue('default', connection=conn0)
#    qs = Queue('slow', connection=conn0)  # how to requeue to slow also?
    for job in qf.jobs:
        try:
            logger.info('Requeuing job %s: filename %s, scan %d, segments, %s' % (job.id, job.args[0]['filename'], job.args[0]['scan'], str(job.args[1])))
        except:
            logger.info('Requeuing job %s: %s' % (job.id, job.args))
        q.enqueue_job(job)
        qf.remove(job)

@click.command()
def clean():
    """ Take jobs from tracking queue and clean them from all queues if they or their dependencies have failed
    """

    q = Queue('default', connection=conn0)
    qf = Queue('failed', connection=conn0)
    jobids = conn.scan(cursor=0, count=trackercount)[1]
    for jobid in jobids:
        job = qf.fetch_job(jobid)
        jobd = qf.fetch_job(jobid).dependency
        if job.is_failed or jobd.is_failed:
            logger.info('Job(s) upstream of %s failed. Removing all dependent jobs from all queues.' % jobid)
            rtutils.removejob(jobid)
            if job.is_failed:
                try:
                    logger.info('Clearning job %s: filename %s, scan %d, segments, %s' % (job.id, job.args[0]['filename'], job.args[0]['scan'], str(job.args[1])))
                except:
                    logger.info('Clearing job %s: %s' % (job.id, job.args))
                q.remove(job)
                qf.remove(job)
            if jobd.is_failed:
                try:
                    logger.info('Clearning job %s: filename %s, scan %d, segments, %s' % (job.id, job.args[0]['filename'], job.args[0]['scan'], str(job.args[1])))
                except:
                    logger.info('Clearing job %s: %s' % (job.id, job.args))
                q.remove(jobd)
                qf.remove(jobd)

@click.command()
@click.argument('qname')
def empty(qname):
    """ Empty qname
    """

    q = Queue(qname, connection=conn0)
    logger.info('Emptying queue %s' % qname)
    for job in q.jobs:
        if isinstance(job.args[0], dict):
            details = (job.args[0]['filename'], job.args[0]['scan'])
        else:
            details = job.args
        logger.info('Removed job %s:, args: %s' % (job.id, str(details)))
        q.remove(job)

@click.command()
def reset():
    """ Reset queues (both dbs)
    """

    for qname in ['default', 'slow', 'failed']:
        q = Queue(qname, connection=conn0)
        logger.info('Emptying queue %s' % qname)
        for job in q.jobs:
            q.remove(job)
            logger.info('Removed job %s' % job.id)

    logger.info('Emptying tracking queue')
    jobids = conn.scan(cursor=0, count=trackercount)[1]
    for jobid in jobids:
        rtutils.removejob(jobid)

@click.command()
@click.argument('filename')
@click.option('--workdir', '-w', help='Directory to put modified version of filename before archiving.', default=None)
@click.option('--goodscanstr', '-g', help='Comma-delimited list of scans to archive. Default is to archive all.', default='')
@click.option('--production', '-p', help='Run code in full production mode (otherwise just runs as test).', is_flag=True, default=False)
@click.option('--bdfdir', help='Directory to look for bdfs.', default='/lustre/evla/wcbe/data/no_archive')
def manualarchive(filename, workdir, goodscanstr, production, bdfdir):
    movetoarchive(filename, workdir, goodscanstr, production, bdfdir)

@click.command()
@click.argument('filename')
@click.option('--slow', help='Time in seconds to integrate to.', default=1)
@click.option('--redishost', help='name of host of redis server. default will run without rq/redis', default=None)
@click.option('--bdfdir', help='Directory to look for bdfs.', default='/lustre/evla/wcbe/data/no_archive')
def slowms(filename, slow, redishost, bdfdir):
    """ Take SDM filename and create MS with integration timescale of slow for all scans.
    Queues to 'slow' queue managed by redishost.
    """

    import sdmreader
    sc,sr = sdmreader.read_metadata(filename, bdfdir=bdfdir)
    logger.info('Creating measurement set for %s, scans %s' % (filename, sc.keys()))

    rtutils.linkbdfs(filename, sc, bdfdir)

    # Submit slow-processing job to our alternate queue.
    allscanstr = ','.join(str(s) for s in sc.keys())
    rtutils.integrate(filename, allscanstr, slow, redishost)
