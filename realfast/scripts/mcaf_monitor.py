#! /usr/bin/env python

# Main controller. Sarah Burke Spolaor June 2015
#
# Based on frb_trigger_controller.py by P. Demorest, 2015/02
#
# Listen for OBS packets having a certain 'triggered archiving'
# intent, and perform some as-yet-unspecified action when these
# are recieved.

import datetime
import os
import logging
import asyncore
import subprocess
import realfast.mcaf_library as mcaf
import click

logging.basicConfig(format="%(asctime)-15s %(levelname)8s %(message)s", level=logging.INFO)
        
mode_default = "intent"
value_default= "realfast"
progname_default = "main_controller"

class FRBController(object):
    """Listens for OBS packets and tells FRB processing about any
    notable scans."""

    def __init__(self, trigger_mode=mode_default, trigger_value=value_default, listen=True, mode="project"):
        # Mode can be project, intent
        self.trigger_mode = trigger_mode
        self.trigger_value = trigger_value
        self.dotrigger = False

    def add_sdminfo(self,sdminfo):
        config = mcaf.MCAST_Config(sdminfo=sdminfo)

        if self.trigger_mode == 'project':
            compString = config.projectID
        elif self.trigger_mode == 'intent':
            compString = config.intentString
        else:
            print ("FRBController didn't understand your trigger mode, %s.\nPlease double-check for valid value." % self.trigger_mode)

        # !!! Wrapper here to deal with potential subscans?

        # Check if MCAST message is simply telling us the obs is finished
        if config.obsComplete:
            logging.info("Received finalMessage=True; This observation has completed.")
        
        # Check if this is one of the scans we're seeking.
        elif self.trigger_value in compString:
            logging.info("Received sought %s: %s" % (self.trigger_mode,compString))

            #!!! THE IF STATEMENT BELOW NEEDS TO BE REMOVED ONCE WE HAVE
            #!!! THE REALFAST INTENT IN PLACE. Its current purpose is
            #!!! for if we're using trigger_mode="project", but we
            #!!! currently want to only trigger off of targets, not
            #!!! the cal scans which will not be running in fast
            #!!! mode. In the future we will not include the
            #!!! "realfast" intent for cal scans/non-fast-dump-mode
            #!!! scans. From Sarah's 27July2015 notes:
            #!!!
            #!!! Add "trigger only on target intent even if user asks
            #!!! for trigger on project". This will be a placeholder
            #!!! for a future catch of some kind of "realfast" intent;
            #!!! i.e. when that special intent starts to exist, we
            #!!! will always necessarily only want to run realfast
            #!!! processing if the realfast intent is on. We should
            #!!! also have some catch to make sure that cals are
            #!!! always run in slow mode even if the realfast intents
            #!!! are run in fast mode. Maybe read intents and if cal
            #!!! and realfast intents are in the same scan, we should
            #!!! do a big "GRRR" kind of print-out?
            if 'TARGET' in config.intentString:
                if verbose:
                    logging.info("Found target in intent %s; will process this scan with realfast." % (config.intentString))

                # If we're not in listening mode, submit the pipeline for this scan as a queue submission.
                job = ['queue_rtpipe.py', config.sdmLocation, '--scans', str(config.scan), '--mode', 'rtsearch', '--paramfile', 'rtparams.py']
                logging.info("Ready to submit scan %d as job %s" % (config.scan, ' '.join(job)))
                if not listen:
                    logging.info("Submitting scan %d as job %s" % (config.scan, ' '.join(job)))
                    subprocess.call(job)
        else:
            logging.info("Received %s: %s" % (self.trigger_mode,compString))
            logging.info("Its BDF is in %s\n" % (config.bdfLocation))

@click.command()
@click.option('--progname', default=progname_default, help='Program name used to trigger action')
@click.option('--trigger_mode', '-m', default=mode_default, help="Trigger on what field? (modes currently accpeted: intent, project). [DEFAULT: %s]" % mode_default)
@click.option('--trigger_value', '-t', default=value_default, help="Triggers if trigger field contains this string. [DEFAULT: %s]" % value_default)
@click.option('--listen', '-l', help="Only listen to multicast, don't launch anything", is_flag=True)
@click.option('--verbose', '-v', help="More verbose output", is_flag=True)
def monitor(progname, trigger_mode, trigger_value, listen, verbose):
    """ Monitor of mcaf observation files. Blocking function.
    """

    logging.info('%s started' % progname)
    logging.info("Trigger mode %s; will trigger on value \"%s\"" % (trigger_mode, trigger_value))

    # Set up verbosity level for log
    if verbose:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logging.debug('Running in verbose mod')

    if listen:
        logging.info('Running in listen-only mode')

    # This starts the receiving/handling loop
    controller = FRBController()
    sdminfo_client = mcaf.SdminfoClient(controller)
    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf
        logging.info('%s got SIGINT, exiting' % progname)
