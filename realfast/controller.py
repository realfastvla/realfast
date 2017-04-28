import logging
import mcast_clients, asyncore

logging.basicConfig(format="%(asctime)-15s %(levelname)8s %(message)s", level=logging.INFO)

class MCAF_controller(object):
    """ Generalize controller to accommodate all four message types.
    Focus on vci and obs docs, which define a complete scan.
    Lots copied from demorest/EVLA_pulsars
    """

    def __init__(self):
        self.sdminfo = None
        self.ant = None
        self.vci = None
        self.obs = None
        self.intents = {}
        logging.info("Initialized controller...")
        
    def add_sdminfo(self, sdminfoxml):
        logging.info("Received sdminfo doc")
        self.sdminfo = mcast_clients.MCAST_Config(sdminfoxml=sdminfoxml)

        if self.is_complete():
            # ** also need to check that sdm/vci are for same scan?
            print('Complete scan. Start processing...')
            print('Subbands:')
            for sub in self.get_subbands():
                print "  IFid=%s swindex=%d sbid=%d bw=%.1f freq=%.1f" % (
                    sub.IFid, sub.swIndex, sub.sbid, sub.bw, sub.sky_center_freq)

    def add_vci(self, vcixml):
        logging.info("Received vci doc")
        self.vci = mcast_clients.MCAST_Config(vcixml=vcixml)

        if self.is_complete():
            # ** also need to check that sdm/vci are for same scan?
            print('Complete scan. Start processing...')
            print('Subbands:')
            for sub in self.get_subbands():
                print "  IFid=%s swindex=%d sbid=%d bw=%.1f freq=%.1f" % (
                    sub.IFid, sub.swIndex, sub.sbid, sub.bw, sub.sky_center_freq)

    def add_obs(self, obsxml):
        logging.info("Received obs doc")
        self.obs = mcast_clients.MCAST_Config(obsxml=obsxml)

    def add_ant(self, antxml):
        logging.info("Received ant doc")
        self.ant = mcast_clients.MCAST_Config(antxml=antxml)

        logging.info("{0} {1} {2} {3} {4} {5}".format(ant.antxml.datasetId, ant.antxml.EopSet, ant.antxml.configuration, 
                                                      ant.antxml.creation, ant.antxml.id, ant.antxml.AntennaProperties))
         
    def has_vci(self):
        return self.vci is not None

    def has_obs(self):
        return self.obs is not None

    def is_complete(self):
        return self.has_vci() and self.has_obs()

    @staticmethod
    def parse_intents(intents):
        d = {}
        for item in intents:
            k, v = item.split("=")
            if v[0] is "'" or v[0] is '"':
                d[k] = ast.literal_eval(v)
                # Or maybe we should just strip quotes?
            else:
                d[k] = v
        return d

    def get_intent(self,key,default=None):
        try:
            return self.intents[key]
        except KeyError:
            return default

    @property
    def Id(self):
        return self.obs.obsxml.configId

    @property
    def datasetId(self):
        return self.obs.obsxml.datasetId

    @property
    def scanNo(self):
        return int(self.obs.obsxml.scanNo)

    @property
    def subscanNo(self):
        return int(self.obs.obsxml.subscanNo)

    @property
    def observer(self):
        return self.get_intent("ObserverName","Unknown")

    @property
    def projid(self):
        return self.get_intent("ProjectID","Unknown")

    @property
    def scan_intent(self):
        return self.get_intent("ScanIntent","None")

    @property
    def nchan(self):
        return int(self.get_intent("PsrNumChan",32))

    @property
    def npol(self):
        return int(self.get_intent("PsrNumPol",4))

    @property
    def source(self):
        return self.obs.obsxml.name.translate(string.maketrans(' *','__'))

    @property
    def ra_deg(self):
        return angles.r2d(self.obs.obsxml.ra)

    @property
    def ra_hrs(self):
        return angles.r2h(self.obs.obsxml.ra)

    @property
    def ra_str(self):
        return angles.fmt_angle(self.ra_hrs, ":", ":").lstrip('+-')

    @property
    def dec_deg(self):
        return angles.r2d(self.obs.obsxml.dec)

    @property
    def dec_str(self):
        return angles.fmt_angle(self.dec_deg, ":", ":")

    @property
    def startLST(self):
        return self.obs.obsxml.startLST * 86400.0

    @property
    def startTime(self):
        try:
            return float(self.obs.obsxml.startTime)
        except AttributeError:
            return 0.0

    @property
    def wait_time_sec(self):
        if self.startTime==0.0:
            return None
        else:
            return 86400.0*(self.startTime - mjd_now())

    @property
    def seq(self):
        return self.obs.obsxml.seq

    @property
    def telescope(self):
        return "VLA"

    def get_sslo(self,IFid):
        """Return the SSLO frequency in MHz for the given IFid.  This will
        correspond to the edge of the baseband.  Uses IFid naming convention 
        as in OBS XML."""
        for sslo in self.obs.obsxml.sslo:
            if sslo.IFid == IFid:
                return sslo.freq # These are in MHz
        return None

    def get_sideband(self,IFid):
        """Return the sideband sense (int; +1 or -1) for the given IFid.
        Uses IFid naming convention as in OBS XML."""
        for sslo in self.obs.obsxml.sslo:
            if sslo.IFid == IFid:
                return sslo.Sideband # 1 or -1
        return None

    def get_receiver(self,IFid):
        """Return the receiver name for the given IFid.
        Uses IFid naming convention as in OBS XML."""
        for sslo in self.obs.obsxml.sslo:
            if sslo.IFid == IFid:
                return sslo.Receiver
        return None

    @staticmethod
    def swbbName_to_IFid(swbbName):
        """Converts values found in the VCI baseBand.swbbName property to
        matching values as used in the OBS sslo.IFid property. 
        
        swbbNames are like AC_8BIT, A1C1_3BIT, etc.
        IFids are like AC, AC1, etc."""

        conversions = {
                'A1C1': 'AC1',
                'A2C2': 'AC2',
                'B1D1': 'BD1',
                'B2D2': 'BD2'
                }

        (bbname, bits) = swbbName.split('_')

        if bbname in conversions:
            return conversions[bbname]

        return bbname

    def get_subbands(self, match_ips=[]):
        """Return a list of SubBand objects for all matching subbands.
        Inputs:
          only_vdif: if True, return only subbands with VDIF output enabled.
                     (default: True)
                     
          match_ips: Only return subbands with VDIF output routed to one of
                     the specified IP addresses.  If empty, all subbands
                     are returned.  non-empty match_ips implies only_vdif
                     always.
                     (default: [])
        """

        # TODO: raise an exception, or just return empty list?
        if not self.is_complete():
            raise RuntimeError("Complete configuration not available: "  
                    + "has_vci=" + self.has_vci() 
                    + " has_obs=" + self.has_obs())

        subs = []

        # NOTE, assumes only one stationInputOutput .. is this legit?
        for baseBand in self.vci.vcixml.stationInputOutput[0].baseBand:
            swbbName = baseBand.swbbName
            IFid = self.swbbName_to_IFid(swbbName)
            for subBand in baseBand.subBand:
                if len(match_ips) or only_vdif:
                    # Need to get at vdif elements
                    # Not really sure what more than 1 summedArray means..
                    for summedArray in subBand.summedArray:
                        vdif = summedArray.vdif
                        if vdif:
                            if len(match_ips):
                                if (vdif.aDestIP in match_ips) or (vdif.bDestIP 
                                        in match_ips):
                                    # IPs match, add to list
                                    subs += [SubBand(subBand,self,IFid,vdif),]
                            else:
                                # No IP list specified, keep all subbands
                                subs += [SubBand(subBand,self,IFid,vdif),]
                else:
                    # No VDIF or IP list given, just keep everything
                    subs += [SubBand(subBand,self,IFid,vdif=None),]

        return subs


class SubBand(object):
    """This class defines relevant info for real-time pulsar processing
    of a single subband.  Most info is contained in the VCI subBand element,
    some is copied out for convenience.  Also the corresponding sky frequency
    is calculated, this depends on the baseBand properties, and LO settings
    (the latter only available in the OBS XML document).  Note, all frequencies
    coming out of this class are in MHz.
    
    Inputs:
        subBand: The VCI subBand element
        config:  The original EVLAConfig object
        vdif:    The summedArray.vdif VCI element (optional)
        IFid:    The IF identification (as in OBS xml)
    """

    def __init__(self, subBand, config, IFid, vdif=None):
        self.IFid = IFid
        self.swIndex = int(subBand.swIndex)
        self.sbid = int(subBand.sbid)
        self.vdif = vdif
        # Note, all frequencies are in MHz here
        self.bw = 1e-6 * float(subBand.bw)
        self.bb_center_freq = 1e-6 * subBand.centralFreq # within the baseband
        ## The (original) infamous frequency calculation, copied here
        ## for posterity:
        ##self.skyctrfreq = self.bandedge[bb] + 1e-6 * self.sideband[bb] * \
        ##                  self.subbands[isub].centralFreq
        self.sky_center_freq = config.get_sslo(IFid) \
                + config.get_sideband(IFid) * self.bb_center_freq
        self.receiver = config.get_receiver(IFid)


if __name__ == '__main__':
    con = MCAF_controller()
    sdminfo_client = mcast_clients.SDMInfoClient(con)
    obs_client = mcast_clients.ObsClient(con)
    vci_client = mcast_clients.VCIClient(con)
    ant_client = mcast_clients.AntClient(con)

    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf on control-C
        logging.info('controller got SIGINT, exiting')
