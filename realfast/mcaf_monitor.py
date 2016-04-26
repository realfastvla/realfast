#! /usr/bin/env python

# Main controller. Sarah Burke Spolaor June 2015
#
# Based on frb_trigger_controller.py by P. Demorest, 2015/02
#
# Listen for OBS packets having a certain 'triggered archiving'
# intent, and perform some as-yet-unspecified action when these
# are recieved.
#

"""
This branch of the code is testing the addition of an option that
archives data as soon as it comes into the CBE, rather than at the end
in queue_monitor.

Need to take care of:

 - Will need no_archive to be functioning so scripts need to include
   this option.

 - Do NOT do any hardlinking at the end in q_m -- otherwise there will
   be double copies pushed into the archive.

 - Delete all BDFs in no_archive at the end.

 - Do not touch bunker or archive.


---------------------- THIS IS THE VERSION I'M DOING -----------------------------

OTHERWISE we can run in an otherproject mode where we ourselves
hardlink a copy of the BDFs to no_archive and then at the end just
delete all the ones from no_archive - never touching the stuff in the
other bunker or archive directories. This would allow us to test on
other datasets, too, without interruption to anyone else.

For this, the following changes are needed:

 - Add option to mcaf_monitor and to queue_monitor.

 - In mcaf_monitor (?), determine BDF list and hardlink them from bunker.

 - In queue_monitor, don't hardlink to archive, just remove BDFs.

 - Error-check for input tags, i.e. make sure mcaf_mon and queue_mon both have the same option set, and make sure that we're not setting both "archive_all" and "handsoff_archive"

 - Throughout ensure compatibility with test and production versions.

 ---------------------------------------------------------------------------------
"""

import datetime
import os
import asyncore
import click
import logging
import sdmreader 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from realfast import queue_monitor, rtutils, mcaf_library

# set up
rtparams_default = os.path.join(os.path.join(os.path.split(os.path.split(mcaf_library.__file__)[0])[0], 'conf'), 'rtpipe_cbe.conf') # install system puts conf files here. used by queue_rtpipe.py
default_bdfdir = '/lustre/evla/wcbe/data/no_archive'
telcaldir = '/home/mchammer/evladata/telcal'  # then yyyy/mm
workdir = os.getcwd()     # assuming we start in workdir
redishost = os.uname()[1]  # assuming we start on redis host


class FRBController(object):
    """Listens for OBS packets and tells FRB processing about any
    notable scans."""

    def __init__(self, intent='', project='', production=False, verbose=False, nrao_controls_archiving=False, rtparams='', slow=0):
        # Mode can be project, intent
        self.intent = intent
        self.project = project
        self.production = production
        self.nrao_controls_archiving = nrao_controls_archiving
        self.verbose = verbose
        self.rtparams = rtparams
        self.slow = slow

    def add_sdminfo(self, sdminfo):
        config = mcaf_library.MCAST_Config(sdminfo=sdminfo)

        # !!! Wrapper here to deal with potential subscans?

        # Check if MCAST message is simply telling us the obs is finished
        if config.obsComplete:
            logger.info("Received finalMessage=True; This observation has completed.")
            # if completing the desired SB, then do a final rsync
            if self.project in config.projectID and self.production:
                logger.info("Final rsync to make workdir copy of SDM %sd complete." % (config.sdmLocation.rstrip('/')))
                rtutils.rsync(config.sdmLocation.rstrip('/'), workdir)  # final transfer to ensure complete SDM in workdir

        # Otherwise check if project is the one we want.
        elif self.project in config.projectID:  # project of interest
            sdmlocation = config.sdmLocation.rstrip('/')
            filename = os.path.join(workdir, os.path.basename(sdmlocation))   # new full-path filename
            scan = int(config.scan)
            bdfloc = os.path.join(default_bdfdir, os.path.basename(config.bdfLocation))

            
            # If we're not controlling the archiving (as would
            # occur if we wanted to make BDFs instantly flowing to
            # the archive, or if we wanted to e.g. test by
            # piggybacking on another (NON-SCIENCE!)
            # observation...
            #
            # We need to then make our own copy in no_archive of
            # the BDF.
            if self.nrao_controls_archiving:
                # Link from bunker to no_archive
                bdfFROM = os.path.join('/lustre/evla/wcbe/data/bunker/',os.path.basename(config.bdfLocation))
                bdfTO   = bdfloc
                if self.production:
                    logger.debug('Hardlinking %s to %s for parallel use by realfast.' % ( bdfFROM, bdfTO ))
                    logger.debug('***********WARNING: QUEUE_MONITOR SHOULD HAVE -N TURNED ON!!!')
                    os.link( bdfFROM, bdfTO )
                else:
                    logger.info('TEST MODE. Would hardlink %s to %s' % ( bdfFROM, bdfTO ))
                    touch( bdfTO + ".test" )

            if self.intent in config.intentString:
                logger.info("Scan %d has desired intent (%s) and project (%s)" % (scan, self.intent, self.project))

                # If we're not in listening mode, prepare data and submit to queue system
                if self.production:
                    assert len(sdmlocation) and isinstance(sdmlocation, str), 'sdmlocation empty or not a string?'
                    assert 'mchammer' not in filename, 'filename %s is SDM original!'

                    # check that SDM is usable by rtpipe. Currently checks spw order and duplicates.
                    if rtutils.check_spw(sdmlocation, scan) and os.path.exists(bdfloc):
                        # 1) copy data into place
                        rtutils.rsync(sdmlocation, workdir)

                        logger.info("Processing sdm %s, scan %d..." % (filename, scan))
                        logger.debug("BDF is in %s\n" % (bdfloc))

                        # 2) find telcalfile (use timeout to wait for it to be written)
                        telcalfile = rtutils.gettelcalfile(telcaldir, filename, timeout=60)

                        # 3) if cal available and bdf exists, submit search job and add tail job to monitoring queue
                        if telcalfile:
                            logger.info('Submitting job to rtutils.search with args: %s %s %s %s %s %s %s %s' % ('default', filename, self.rtparams, '', str([scan]), telcalfile, redishost, default_bdfdir))
                            lastjob = rtutils.search('default', filename, self.rtparams, '', [scan], telcalfile=telcalfile, redishost=redishost, bdfdir=default_bdfdir)
                            rtutils.addjob(lastjob.id)                            
                        else:
                            logger.info('No calibration available. No job submitted.')
                    else:
                        logger.info("Not submitting scan %d of sdm %s. bdf not found or cannot be processed by rtpipe." % (scan, os.path.basename(sdmlocation)))                    

            # each CAL and TARGET scan gets integrated to MS for slow transients search, if in production mode and slow timescale set
            if (self.slow > 0) and ( ('CALIBRATE' in config.intentString) or ('TARGET' in config.intentString) and self.production):
                rtutils.rsync(sdmlocation, workdir)
                logger.info('Creating measurement set for %s scan %d' % (filename, scan))
                sc,sr = sdmreader.read_metadata(filename, scan, bdfdir=default_bdfdir)
                rtutils.linkbdfs(filename, sc, default_bdfdir)
                rtutils.integrate(filename, str(scan), self.slow, redishost)                    
                    
@click.command()
@click.option('--intent', '-i', default='', help='Intent to trigger on')
@click.option('--project', '-p', default='', help='Project name to trigger on')
@click.option('--production', help='Run the code (not just print, etc)', is_flag=True)
@click.option('--verbose', '-v', help='More verbose output', is_flag=True)
@click.option('--nrao_controls_archiving', '-N', help='NRAO controls archiving; i.e. we make our own BDF hardlinks to no_archive and do no archiving at the end. If this is selected, queue_monitor MUST ALSO be run with this same option set.', is_flag=True)
@click.option('--rtparams', help='Parameter file for rtpipe. Default is rtpipe_cbe_conf.', default=rtparams_default)
@click.option('--slow', help='Integrate scans to MS with ints of this many seconds.', default=0)
def monitor(intent, project, production, verbose, nrao_controls_archiving, rtparams, slow):
    """ Monitor of mcaf observation files. 
    Scans that match intent and project are searched (if in production mode).
    Blocking function.
    """

    # Set up verbosity level for log
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    logger.info('mcaf_monitor started')
    logger.info('Looking for intent = %s, project = %s' % (intent, project))
    logger.debug('Running in verbose mode')

    if not production:
        logger.info('Running in test mode')
    else:
        logger.info('Running in production mode')

    if nrao_controls_archiving:
        logger.info('*** NRAO CONTROLLING ARCHIVING. ***')
        logger.info('*** Will link and clean BDFs in no_archive directory without ever touching archive directory.')

    # This starts the receiving/handling loop
    controller = FRBController(intent=intent, project=project, production=production, verbose=verbose, nrao_controls_archiving=nrao_controls_archiving, rtparams=rtparams, slow=slow)
    sdminfo_client = mcaf_library.SdminfoClient(controller)
    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf
        logger.info('Escaping mcaf_monitor')

def testrtpipe(filename, paramfile):
    """ Function for a quick test of rtpipe and queue system
    filename should have full path.
    """

    import sdmreader
    sc,sr = sdmreader.read_metadata(filename, bdfdir=default_bdfdir)
    scan = sc.keys()[0]
    telcalfile = rtutils.gettelcalfile(telcaldir, filename, timeout=60)
    lastjob = rtutils.search('default', filename, paramfile, '', [scan], telcalfile=telcalfile, redishost=redishost, bdfdir=default_bdfdir)
    return lastjob

# Temporary method for creating an empty file.                                                                                                                
def touch(path):
    with open(path, 'a'):
        os.utime(path, None)
