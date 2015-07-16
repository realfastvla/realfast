from redis import Redis
from rq.queue import Queue
from rq.registry import DeferredJobRegistry, FinishedJobRegistry

conn = Redis()

def getdeferred(q):
    q = Queue('monitor', connection=conn)
    return DeferredJobRegistry(name=q.name,connection=q.connection).get_job_ids()

def getfinished():
    q = Queue('monitor', connection=conn)
    return FinishedJobRegistry(name=q.name,connection=q.connection).get_job_ids()

def enqueuefinished():
    """ Takes finished jobs and puts them back in.
    Note this grows finished registry exponentially. Need to have removal, too!
    """

    q = Queue('monitor', connection=conn)
    reg = FinishedJobRegistry(name=q.name, connection=q.connection)

    finishedids = reg.get_job_ids()
    for jobid in finishedids:
        job = q.fetch_job(jobid)
        q.enqueue_call(job.func, job.args, timeout=24*3600)

def addmonitorjob(jobid):
    """ Uses monitor queue to track job at jobid on default queue.
    Calls is_finished, which reutrns job.is_finished on default queue.
    """

    q = Queue('monitor', connection=conn)
    q.enqueue_call(func=is_finished, args=(jobid,), timeout=24*3600, result_ttl=24*3600)  # these will start when 'monitorjob' is crated, enqueued, and completed (see monitor.py)

def is_finished(jobid):
    """ Checks on status of job at jobid on default queue.
    """

    checkq = Queue('default', connection=conn)
    job = checkq.fetch_job(jobid)
    return job.is_finished

