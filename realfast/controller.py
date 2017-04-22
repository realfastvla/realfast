import logging
import mcaf_library, asyncore

logging.basicConfig(format="%(asctime)-15s %(levelname)8s %(message)s", level=logging.INFO)

class MCAST_Config(object):
    """
    Wrap any one of the multicast messages used at the VLA
    """

    def __init__(self, sdminfoxml=None, obsxml=None, antxml=None, vcixml=None):
        self.sdminfoxml = sdminfoxml
        self.obsxml = obsxml
        self.antxml = antxml
        self.vcixml = vcixml


class MCAF_controller(object):
    def __init__(self):
        self.sdminfo = None
        self.ant = None
        self.vci = None
        self.obs =None
        logging.info("Initialized controller...")
        
    def add_sdminfo(self, sdminfoxml):
        sdminfo = MCAST_Config(sdminfoxml=sdminfoxml)
        logging.info("Wrapped sdminfoxml")
        logging.info("{0}".format([pr for pr in dir(sdminfoxml) if '__' not in pr]))

    def add_vci(self, vcixml):
        vci = MCAST_Config(vcixml=vcixml)
        logging.info("Wrapped vcixml")
        logging.info("{0}".format([pr for pr in dir(vcixml) if '__' not in pr]))

    def add_obs(self, obsxml):
        obs = MCAST_Config(obsxml=obsxml)
        logging.info("Wrapped obsxml")
        logging.info("{0}".format([pr for pr in dir(obsxml) if '__' not in pr]))

    def add_ant(self, antxml):
        ant = MCAST_Config(antxml=antxml)
        logging.info("Wrapped antxml")
        logging.info("{0}".format([pr for pr in dir(antxml) if '__' not in pr]))



if __name__ == '__main__':
    sdmcon = MCAF_controller()
    antcon = MCAF_controller()
    vcicon = MCAF_controller()
    obscon = MCAF_controller()
    sdminfo_client = mcaf_library.SDMInfoClient(sdmcon)
    obs_client = mcaf_library.ObsClient(obscon)
    vci_client = mcaf_library.VCIClient(vcicon)
    ant_client = mcaf_library.AntClient(antcon)

    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf on control-C
        logging.info('controller got SIGINT, exiting')
