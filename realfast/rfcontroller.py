from evla_mcast.controller import Controller
import rfpipe

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('rfpipe')

distributed_host = 'cbe-node-01'
vys_cfile = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'
default_preffile = '/lustre/evla/test/realfast/realfast.yml'
#default_scantime = 60 # in seconds
default_vys_timeout = 10 # in seconds

class realfast_controller(Controller):

    def __init__(self, preffile=default_preffile, inprefs={}, vys_timeout=default_vys_timeout):
        """ Creates controller object that can act on a scan configuration.
        Inherits a "run" method that starts asynchronous operation.
        """

        super(realfast_controller, self).__init__()
        self.preffile = preffile
        self.inprefs = inprefs
#        self.scantime = scantime
        self.vys_timeout = vys_timeout

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        logger.info('Docs received (ant, vci, obs): {0}, {1}, {2}'.format(config.ant, config.vci, config.obs))
        try:
            logger.info('Generating rfpipe state...')
            st = rfpipe.state.State(config=config, preffile=self.preffile, inprefs=self.inprefs) #, inmeta=inmeta)
            
            logger.info('Starting pipeline...')
            fut = rfpipe.pipeline.pipeline_scan(st, host=distributed_host, cfile=vys_cfile, vys_timeout=self.vys_timeout)
        except KeyError as exc:
            logger.warn('KeyError in parsing VCI? {0}'.format(exc))
