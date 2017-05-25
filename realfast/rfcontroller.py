import logging

from evla_mcast.controller import Controller
import rfpipe

class realfast_controller(Controller):

    def __init__(self, preffile=None, inprefs={}):
        super(realfast_controller, self).__init__()
        self.preffile = preffile
        self.inprefs = inprefs

    def handle_config(self, config):
        """ Triggered when obs comes in.
        Downstream logic starts here.
        """

        meta = rfpipe.metadata.config_metadata(config)
        logging.info('config metadata ingested by rfpipe'.format(meta))
        st = rfpipe.state.State(preffile=self.preffile, inmeta=meta, inprefs=self.inprefs)
        logging.info('rfpipe state generated'.format(meta))
