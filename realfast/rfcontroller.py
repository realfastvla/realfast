import logging

from evla_mcast.controller import Controller
import rfpipe

class realfast_controller(Controller):

    def __init__(self, preffile=None, inprefs={}):
        super(realfast_controller, self).__init__()
        self.preffile = preffile
        self.inprefs = inprefs
        self.fut = []

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        logging.info('ingesting config metadata to rfpipe')
        meta = rfpipe.metadata.config_metadata(config)

        logging.info('generating rfpipe state')
        st = rfpipe.state.State(preffile=self.preffile, inmeta=meta, inprefs=self.inprefs)

        fut = rfpipe.pipeline.pipeline_vys2(st, 0)
        logging.info('started pipeline')

        self.futures.append(fut)
