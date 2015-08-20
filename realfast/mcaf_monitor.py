#! /usr/bin/env python

# Main controller. Sarah Burke Spolaor June 2015
#
# Based on frb_trigger_controller.py by P. Demorest, 2015/02
#
# Listen for OBS packets having a certain 'triggered archiving'
# intent, and perform some as-yet-unspecified action when these
# are recieved.
#

import datetime
import os
import asyncore
import click
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from realfast import queue_monitor, rtutils, mcaf_library

# set up
rtparams_default = os.path.join(os.path.join(os.path.split(os.path.split(mcaf_library.__file__)[0])[0], 'conf'), 'rtpipe_cbe.conf') # install system puts conf files here. used by queue_rtpipe.py
telcaldir = '/home/mchammer/evladata/telcal'  # then yyyy/mm
workdir = os.getcwd()     # assuming we start in workdir
redishost = os.uname()[1]  # assuming we start on redis host

class FRBController(object):
    """Listens for OBS packets and tells FRB processing about any
    notable scans."""

    def __init__(self, intent='', project='', listen=True, verbose=False, rtparams=''):
        # Mode can be project, intent
        self.intent = intent
        self.project = project
        self.listen = listen
        self.verbose = verbose
        self.rtparams = rtparams

    def add_sdminfo(self, sdminfo):
        config = mcaf_library.MCAST_Config(sdminfo=sdminfo)

        # !!! Wrapper here to deal with potential subscans?

        # Check if MCAST message is simply telling us the obs is finished
        if config.obsComplete:
            logger.info("Received finalMessage=True; This observation has completed.")

        elif self.intent in config.intentString and self.project in config.projectID:
            logger.info("Scan %d has desired intent (%s) and project (%s)" % (config.scan, self.intent, self.project))
            logger.debug("BDF is in %s\n" % (config.bdfLocation))

            # If we're not in listening mode, prepare data and submit to queue system
            if not self.listen:
                filename = config.sdmLocation.rstrip('/')
                scan = int(config.scan)

                assert len(filename) and isinstance(filename, str), 'Filename empty or not a string?'

                # check that SDM is usable by rtpipe. Currently checks spw order and duplicates.
                if rtutils.check_spw(filename, scan):
                    logger.info("Processing sdm %s, scan %d..." % (os.path.basename(filename)), scan)

                    # 1) copy data into place
                    rtutils.rsyncsdm(filename, workdir)
                    filename = os.path.join(workdir, os.path.basename(filename))   # new full-path filename
                    assert 'mchammer' not in filename, 'filename %s is SDM original!'

                    # 2) find telcalfile (use timeout to wait for it to be written)
                    telcalfile = rtutils.gettelcalfile(telcaldir, filename, timeout=60)

                    # 3) submit search job and add tail job to monitoring queue
                    if telcalfile:
                        logger.info('Submitting job to rtutils.search with args: %s %s %s %s %s %s %s %s' % ('default', filename, self.rtparams, '', str([scan]), telcalfile, redishost, os.path.dirname(config.bdfLocation.rstrip('/'))))
                        lastjob = rtutils.search('default', filename, self.rtparams, '', [scan], telcalfile=telcalfile, redishost=redishost, bdfdir=os.path.dirname(config.bdfLocation.rstrip('/')))
                        queue_monitor.addjob(lastjob.id)
                    else:
                        logger.info('No calibration available. No job submitted.')
                else:
                    logger.info("Not submitting scan %d of sdm %s. spw order strange or duplicates found." % (scan, os.path.basename(filename)))                    

@click.command()
@click.option('--intent', '-i', default='', help='Intent to trigger on')
@click.option('--project', '-p', default='', help='Project name to trigger on')
@click.option('--listen/--do', '-l', help='Only listen to multicast or actually do work?', default=True)
@click.option('--verbose', '-v', help='More verbose output', is_flag=True)
@click.option('--rtparams', help='Parameter file for rtpipe. Default is rtpipe_cbe_conf.', default=rtparams_default)
def monitor(intent, project, listen, verbose, rtparams):
    """ Monitor of mcaf observation files. 
    Scans that match intent and project are searched (unless --listen).
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

    if listen:
        logger.info('Running in listen mode')
    else:
        logger.info('Running in do mode')

    # This starts the receiving/handling loop
    controller = FRBController(intent=intent, project=project, listen=listen, verbose=verbose, rtparams=rtparams)
    sdminfo_client = mcaf_library.SdminfoClient(controller)
    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf
        logger.info('Escaping mcaf_monitor')
