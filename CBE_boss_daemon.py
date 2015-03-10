#!/usr/bin/env python2.7
#
#
# Still need to be resolved:
#  - what is the naming convention for the slave nodes?
#
#
# 
print "Please be patient while libraries load...\n"
print "RQ..."
from rq import Queue, Connection
print "rtpipe..."
import rtpipe.RT as rt
import rtpipe.parsesdm as ps
print "subprocess..."
from subprocess import call
print "OS and SDMreader..."
import os, time, argparse
from sdmreader import sdmreader
print "Asyncore..."
import asyncore  # To monitor for new SDM asynchronously
print "pyinotify..."
import pyinotify # Wrapper on inotify to monitor SDM directory

print "Finally done importing modules!\n";
   
# parse input to get name
parser = argparse.ArgumentParser()
parser.add_argument("partialname", help="String to match to SDM file name")
args = parser.parse_args(); partialname = args.partialname

"""
This job should run "all the time" in the background on a control
node, waiting for appropriate data to come in.

Still to do:

1. Clean up: Kill any previously abandoned queue processes and report
   the existence of any derelict data.

2. Scan-by-scan basis!

"""



"""
CASEY QUESTIONS

0. Let's fix our git repo.

!!!SEARCH PROGRAM CODE IN A FILE NAME!!!

MAKE IT TAKE STRING TO SEARCH FOR. So it can be run by the queue.

0. What's written first, the BDF or the SDM?

1. Do we want to trigger processing per SB or per scan for the test?
   This will change whether I watch a directory and/or a file.
   NOTE: from discussion with Paul 

2. I think I'm not dealing with scandict/scanintent appropriately?
   Also I think it needs a "search per scan", i.e. the SDM won't get
   the intent, the scans should get an intent?

5. How do we call rtpipe within this code? Or should we just wrap
   rtpipe in this boss code?

3. All the "NOTE"s in my code!

4. For now, do we want to remove the archive hardlink as soon as the
   data gets in? Ultimately we should liaise with Martin to have the
   hardlink not written for our obs intent.

6. How do we deal with calibration for this online system???

"""




# - - - - - - - - - - - - - - - -
# Set up variabiles
# - - - - - - - - - - - - - - - -
SDM_workdir = "/lustre/aoc/projects/fasttransients/code/RT_TEST2/sdmdir/"
#SDM_workdir = "/home/mchammer/evla/mcaf/workspace/"
BDF_workdir = "/lustre/evla/wcbe/data/bunker/"
SDM_archdir = "/home/mchammer/evla/sdm/"
BDF_archdir = "/lustre/evla/wcbe/data/archive/"
our_intent  = "TARGET"
our_source  = "B0355"

# - - - - - - - - - - - - - - - -
# Determine node availability
# (Paul: "Should be hard-wired")
# - - - - - - - - - - - - - - - -
"""
NOTE: Need to determine which ~8-10 nodes we can hardwire into the
code for our tests.
"""
available_nodes = "nmpost035".split(" ")


# - - - - - - - - - - - -
# Functions for work to be triggered
# - - - - - - - - - - - -

def search(sdmfile, scan, q):
    """ Search for transients in scan
    """

    # parameters of search
    chans = range(256)
    spw = range(2)
    nsegments = 100
    dmarr = [0]
    dtarr = [1]
    timesub = 'mean'
    searchtype = 'image1'
    sigma_image1 = 8.
    flagmode = 'standard'
    uvres = 0
    npix = 0

    # queue jobs
    joblist = []
    print 'Getting metadata for %s, scan %d' % (sdmfile, scan)
    state = ps.get_metadata(sdmfile, scan, chans=chans, spw=spw)
    rt.set_pipeline(state, nsegments=nsegments, dmarr=dmarr, dtarr=dtarr, timesub=timesub, sigma_image1=sigma_image1, flagmode=flagmode, searchtype=searchtype, uvres=uvres, npix=npix)
    print 'Sending %d segments to queue' % (state['nsegments'])
#    for segment in range(state['nsegments']):
    for segment in [50]:
        joblist.append(q.enqueue_call(func=rt.pipeline, args=(state, segment), timeout=24*3600, result_ttl=24*3600))

    return joblist


def find_newest_file(directory, partial_file_name):
    files = os.listdir(directory)
    files = filter(lambda x:x.find(partial_file_name) > -1, files)
    name_n_timestamp = dict ([(x,os.stat(directory+x).st_mtime) for x in files])
    return max(name_n_timestamp, key=lambda k: name_n_timestamp.get(k))


class newSDM(pyinotify.ProcessEvent):
    def __init__(self, q, partialname):
        pyinotify.ProcessEvent.__init__(self)
        self.q = q
        self.partialname = partialname

    def process_IN_ONLYDIR(self, event):
        self.process_IN_CREATE(self, event)

    def process_IN_CREATE(self, event):
        """ Find latest file in directory; read metadata. """
        print "Looking for newest file..."
        SDM_file = find_newest_file(SDM_workdir,self.partialname) # Add name mask in the quotes
        SDM_file = SDM_workdir + SDM_file
        print "Found SDM file " + SDM_file

        time.sleep(2) # !!! NEED TO EVENTUALLY FIX THIS. Currently
                        # it's here so the full SDM file can be
                        # written before we try to read it... Maybe we
                        # can instead do something like wait for the
                        # last-written xml file to exist so we know
                        # the SDM is done writing?
        (scandict, sourcedict) = sdmreader.read_metadata(SDM_file)
        
        """ Does this SDM have the right intent? """
        joblist_all = []
        for scan in scandict:
            if (our_intent in scandict[scan]['intent']) and (our_source in scandict[scan]['source']):
                print "Found good scan " + str(scan)
                
                # - - - - - - - - - - - - - - - -
                # When relevant SDM file exists, start processing.
                # - - - - - - - - - - - - - - - -
                # Search per scan. Multiple jobs per scan. Returns
                # joblist, which needs to be monitored for return
                # value (number of candidates).

                joblist = search(SDM_file, scan, self.q)
                print "joblist made"
                joblist_all += joblist
                
        # - - - - - - - - - - - - - - - -
        # Wait for all the results to come back from the slaves
        # - - - - - - - - - - - - - - - -
        print 'Submitted %d jobs' % len(joblist_all)
        while len(joblist_all):
            for i in range(len(joblist_all)):
                if joblist_all[i].is_finished:
                    job = joblist_all.pop(i)
                    print '\t job %d result: %s' % (i, job.return_value)
                    break
                elif joblist_all[i].is_failed:
                    job = joblist_all.pop(i)
                    print '\t job %d: uh oh...'
                    break
            time.sleep(1)
        print 'all done with this event!'

        # - - - - - - - - - - - - - - - -
        # [OPTIONAL] Do candidate auto-assessment/rejection
        # - - - - - - - - - - - - - - - -
        
        # - - - - - - - - - - - - - - - -
        # Dissect scans as appropriate OR
        # Write a scanlist for Rich's or Bryan's code to ingest.
        # - - - - - - - - - - - - - - - -
        
        # - - - - - - - - - - - - - - - -
        # [POSSIBLY] Hardlink savescans to the archive directories.
        # - - - - - - - - - - - - - - - -



if __name__ == '__main__':
    # - - - - - - - - - - - - - - - -
    # Kill and start queue
    # - - - - - - - - - - - - - - - -
    """
    Note: Before (re)starting queue, we should perform a check for
    derelict data on each of the nodes.  This should check in our
    (presumably set) scratch/processing directory on the slave nodes to
    see if there's anything in there.
    """
    
    print "Starting queue...\n"
    try:
        call(['/users/claw/code/vlart/rqmanage.sh','stop']+available_nodes)#,shell=True)
        call(['/users/claw/code/vlart/rqmanage.sh','start']+available_nodes)#,shell=True)
    except OSError as e:
        print "\n\tError in queue initialization with rqmanage.sh: ", e

    with Connection():
        q = Queue('low')
        print "q is set."

        # - - - - - - - - - - - - - - - -
        # Watch obsdocs or SDMs for start of appropriate intent.
        # - - - - - - - - - - - - - - - -
        """ 
        NOTE probably there is a better way to code this than I have;
        currently I'm just putting all the work in the "new file" event.
        i.e. perhaps I should have the new file event call another function
        that runs the brunt of the code. I also need better error handling in
        the asynchronous loop, I think...
        """
        
        """ Watch manager """
        watchman = pyinotify.WatchManager()
        """ Set to watch for new files """
        newfilewatch = pyinotify.IN_CREATE | pyinotify.IN_ONLYDIR
        
        notifier = pyinotify.AsyncNotifier(watchman, newSDM(q, partialname))
        
        """ Watch SDM directory for new files; non-recursive!  (doesn't watch subdirectories) """
        wdd = watchman.add_watch(SDM_workdir, newfilewatch)
        
        print "entering async loop..."
        try:
            asyncore.loop()
        except KeyboardInterrupt:
            print "\n\nKeyboard interrupt received. Exiting.\n"


"""
NOTE HERE ARE THE CHANGES TO MAKE FOR RUNNING IN A ROBUST NON-DEDICATED MODE

1. Dynamically determine usage of slave nodes. This will depend on how
   many nodes the standard correlation intends to take for that
   SB. NOTE THIS SEEMS TO NOT BE SOMETHING WE NEED TO DO; PAUL SAYS
   IT'S ALWAYS JUST 8 NODES AND THAT WHICH NODES ARE SET BY HAND. BUT
   IS THAT ACTUALLY TRUE? NEED TO VERIFY.

2. Determine DM steps based on metadata. Do this either in the
   pipeline itself, or have the CBE boss calculate it from the
   meta-data and write a reference file for that frequency set-up for
   future use.

3. As above for parsing SDM for metadata!
"""
