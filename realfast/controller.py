import logging

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
        self.obs =Non
        print("Initialized controller...")
        
    def add_sdminfo(self, sdminfoxml):
        sdminfo = mcaf_library.MCAST_Config(sdminfoxml=sdminfoxml)
        print("Wrapped sdminfoxml")

    def add_vci(self, vcixml):
        vci = mcaf_library.MCAST_Config(vcixml=vcixml)
        print("Wrapped vcixml")

    def add_obs(self, obsxml):
        obs = mcaf_library.MCAST_Config(obsxml=obsxml)
        print("Wrapped obsxml")

    def add_ant(self, antxml):
        ant = mcaf_library.MCAST_Config(antxml=antxml)
        print("Wrapped antxml")


if __name__ == '__main__':
    sdmcon = MCAF_controller()
    antcon = MCAF_controller()
    vcicon = MCAF_controller()
    obscon = MCAF_controller()
    sdminfo_client = SDMInfoClient(sdmcon)
    obs_client = ObsClient(obscon)
    vci_client = VCIClient(vcicon)
    ant_client = AntClient(antcon)

    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf on control-C
        logging.info('got SIGINT, exiting')
