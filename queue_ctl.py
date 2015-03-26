#!/usr/bin/env python2.7
#
# Control queue

from rq import Queue, Connection
import os, glob, time, argparse, pickle, string

parser = argparse.ArgumentParser()
parser.add_argument("mode", help="'clear', 'failed', 'resubmit'")
args = parser.parse_args(); mode = args.mode

if __name__ == '__main__':

    # connect
    with Connection():

        if mode == 'clear':
            q = Queue('high')
            q.empty()
            q = Queue('low')
            q.empty()
            q = Queue('joblists')
            q.empty()
            q = Queue('failed')
            q.empty()
            q = Queue('default')
            q.empty()

        elif mode == 'failed':
            q = Queue('failed')
            print 'Failed queue:'
            print q.jobs
            for i in range(len(q.jobs)):
                print 'Failure %d' % i
                print q.jobs[i].exc_info

        elif mode == 'resubmit':
            q = Queue('failed')
            joblist = q.jobs
            print 'Resubmitting %d jobs to resubmit' % len(joblist)
            q = Queue('low')
            for job in joblist:
                q.enqueue_job(job)
