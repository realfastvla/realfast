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
import logging
import asyncore
import subprocess
import realfast.mcaf_library as mcaf
import click

logging.basicConfig(format="%(asctime)-15s %(levelname)8s %(message)s", level=logging.INFO)
confloc = os.path.join(os.path.split(os.path.split(mcaf.__file__)[0])[0], 'conf')   # install system puts conf files here. used by queue_rtpipe.py

class FRBController(object):
    """Listens for OBS packets and tells FRB processing about any
    notable scans."""

    def __init__(self, intent='', project='', listen=True, verbose=False):
        # Mode can be project, intent
        self.intent = intent
        self.project = project
        self.listen = listen
        self.verbose = verbose

    def add_sdminfo(self, sdminfo):
        config = mcaf.MCAST_Config(sdminfo=sdminfo)

        # !!! Wrapper here to deal with potential subscans?

        # Check if MCAST message is simply telling us the obs is finished
        if config.obsComplete:
            logging.info("Received finalMessage=True; This observation has completed.")

        elif self.intent in config.intentString and self.project in config.projectID:
            logging.info("Received sought intent %s and project %s" % (self.intent, self.project))
            logging.debug("BDF is in %s\n" % (config.bdfLocation))

            # If we're not in listening mode, submit the pipeline for this scan as a queue submission.
            job = ['queue_rtpipe.py', config.sdmLocation, '--scans', str(config.scan), '--mode', 'rtsearch', '--paramfile', os.path.join(confloc, 'rtpipe_cbe.conf')]
            logging.info("Ready to submit scan %d as job %s" % (config.scan, ' '.join(job)))
            if not self.listen:
                logging.info("Submitting scan %d as job %s" % (config.scan, ' '.join(job)))
                subprocess.call(job)

@click.command()
@click.option('--intent', '-i', default='', help="Intent to trigger on")
@click.option('--project', '-p', default='', help="Project name to trigger on")
@click.option('--listen', '-l', help="Only listen to multicast, don't launch anything", is_flag=True)
@click.option('--verbose', '-v', help="More verbose output", is_flag=True)
def monitor(intent, project, listen, verbose):
    """ Monitor of mcaf observation files. 
    Scans that match intent and project are searched (unless --listen).
    Blocking function.
    """

    logging.info('mcaf_monitor started')
    logging.info("Looking for intent = %s, project = %s" % (intent, project))

    # Set up verbosity level for log
    if verbose:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logging.debug('Running in verbose mode')

    if listen:
        logging.info('Running in listen-only mode')

    # This starts the receiving/handling loop
    controller = FRBController(intent=intent, project=project, listen=listen, verbose=verbose)
    sdminfo_client = mcaf.SdminfoClient(controller)
    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf
        logging.info('Escaping mcaf_monitor')
