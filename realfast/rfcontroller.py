from evla_mcast.controller import Controller
import rfpipe

import logging
logger = logging.getLogger(__name__)

distributed_host = 'cbe-node-01'
vys_cfile = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'
default_preffile = '/lustre/evla/test/realfast/realfast.yml'
default_scantime = 60 # in seconds
default_vys_timeout = 10 # in seconds

class realfast_controller(Controller):

    def __init__(self, scantime=default_scantime, preffile=default_preffile, inprefs={}, vys_timeout=default_vys_timeout):
        """ Creates controller object that can act on a scan configuration.
        Inherits a "run" method that starts asynchronous operation.
        scantime length in seconds to try to catch data with vysmaw.
        """

        super(realfast_controller, self).__init__()
        self.preffile = preffile
        self.inprefs = inprefs
        self.scantime = scantime
        self.vys_timeout = vys_timeout

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        logger.debug('doc types (ant, vci, obs) values: {0}, {1}, {2}'.format(type(config.ant), type(config.vci), type(config.obs)))
        if config.is_complete():
            # calc nints (this can be removed once scan config includes scan end time)
            try:
                subband0 = config.get_subbands()[0]
                inttime = subband0.hw_time_res  # assumes that vys stream comes after hw integration
                nints = int(self.scantime/inttime)
                logger.info('Pipeline will be set to catch {0} s of data ({1} integrations)'.format(self.scantime, nints))
                inmeta = {'nints': nints}

                logger.info('Generating rfpipe state...')
                st = rfpipe.state.State(config=config, preffile=self.preffile, inprefs=self.inprefs, inmeta=inmeta)
            
                logger.info('Starting pipeline...')
                fut = rfpipe.pipeline.pipeline_scan(st, host=distributed_host, cfile=vys_cfile, vys_timeout=self.vys_timeout)
            except KeyError as exc:
                logger.warn('KeyError in parsing VCI? {0}'.format(exc))
            
        else:
            logger.info('Waiting for complete scan configuration.')
