import logging

from evla_mcast.controller import Controller
import rfpipe

distributed_host = 'cbe-node-01'
vys_cfile = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'
default_preffile = '/lustre/evla/test/realfast/realfast.yml'
default_scantime = 60 # in seconds

class realfast_controller(Controller):

    def __init__(self, scantime=default_scantime, preffile=default_preffile, inprefs={}):
        """ Creates controller object that can act on a scan configuration.
        Inherits a "run" method that starts asynchronous operation.
        scantime length in seconds to try to catch data with vysmaw.
        """

        super(realfast_controller, self).__init__()
        self.preffile = preffile
        self.inprefs = inprefs
        self.scantime = scantime

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        # calc nints (this can be removed once scan config includes scan end time)
        subband0 = config.get_subbands()[0]
        inttime = subband0.hw_time_res  # assumes that vys stream comes after hw integration
        nints = int(scantime/inttime)
        logging.info('Pipeline will be set to catch {0} s of data ({1} integrations)'.format(self.scantime, nints))
        inmeta = {'nints': nints}

        logging.info('Generating rfpipe state...')
        st = rfpipe.state.State(config=config, preffile=self.preffile, inprefs=self.inprefs, inmeta=inmeta)

        logging.info('Starting pipeline...')
        fut = rfpipe.pipeline.pipeline_scan(st, host=distributed_host, cfile=vys_cfile)


