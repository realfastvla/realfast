import os
import struct
import logging
import asyncore, socket
import sdminfoxml_parser

logger = logging.getLogger(__name__)

class McastClient(asyncore.dispatcher):
    """Generic class to receive the multicast XML docs."""

    def __init__(self, group, port, name=""):
        asyncore.dispatcher.__init__(self)
        self.name = name
        self.group = group
        self.port = port
        addrinfo = socket.getaddrinfo(group, None)[0]
        self.create_socket(addrinfo[0], socket.SOCK_DGRAM)
        self.set_reuse_addr()
        self.bind(('',port))
        mreq = socket.inet_pton(addrinfo[0],addrinfo[4][0]) \
                + struct.pack('=I', socket.INADDR_ANY)
        self.socket.setsockopt(socket.IPPROTO_IP, 
                socket.IP_ADD_MEMBERSHIP, mreq)
        self.read = None

    def handle_connect(self):
        logger.debug('connect %s group=%s port=%d' % (self.name, 
            self.group, self.port))

    def handle_close(self):
        logger.debug('close %s group=%s port=%d' % (self.name, 
            self.group, self.port))

    def writeable(self):
        return False

    def handle_read(self):
        self.read = self.recv(100000)
        logger.debug('read ' + self.name + ' ' + self.read)
        try:
            self.parse()
        except Exception as e:
            logger.exception("error handling '%s' message" % self.name)

    def handle_error(self, type, val, trace):
        logger.error('unhandled exception: ' + repr(val))


class SdminfoClient(McastClient):
    """Receives sdminfo XML, which is broadcast when the BDF is available.

    If the controller input is given, the
    controller.add_sdminfo(sdminfo) method will be called for every
    document received. Controller is defined as a class in the main
    controller script, and runs job launching.
    """

    def __init__(self,controller=None):
        McastClient.__init__(self,'239.192.5.2',55002,'sdminfo')
        self.controller = controller

    def parse(self):
        sdminfo = sdminfoxml_parser.parseString(self.read)
        logger.info("Read sdminfo datasetId='%s' scanNo=%d intents=%s" % (sdminfo.datasetId,
            sdminfo.scanNumber, str(sdminfo.scanIntents)))
        if self.controller is not None:
            self.controller.add_sdminfo(sdminfo)


#A dumbed down version of EVLAconfig just for reading SDM info
class MCAST_Config(object):
    """
    This class at the moment just returns info from the SDM
    document. It can easily be expanded to include further information
    from e.g. the OBS doc or VCI doc (e.g. info on observation length,
    SPWs, antenna config, and so forth).
    """

    def __init__(self, sdminfo=None):
        self.set_sdminfo(sdminfo)

    def is_complete(self):
        return self.sdminfo is not None

    def set_sdminfo(self,sdminfo):
        self.sdminfo = sdminfo

    # July 2015:
    # Rich will soon switch from datasetID to datasetId. This takes
    # into account both possibilities
    @property
    def projectID(self):
        if self.sdminfo.datasetId is not None:
            return self.sdminfo.datasetId
        else:
            return self.sdminfo.datasetID

    @property
    def telescope(self):
        return "VLA"

    @property
    def scan(self):
        return self.sdminfo.scanNumber

    @property
    def subscan(self):
        return self.sdminfo.subscanNumber

    @property
    def bdfLocation(self):
        bdfdir = '/lustre/evla/wcbe/data/no_archive/'
        return os.path.join(bdfdir, os.path.basename(self.sdminfo.bdfLocation))

    @property
    def sdmLocation(self):
        return self.sdminfo.sdmLocation

    @property
    def intentString(self):
        return self.sdminfo.scanIntents

    @property
    def obsComplete(self):
        return self.sdminfo.finalMessage


# This is how comms would be used in a program.  Note that no controller
# is passed, so the only action taken here is to print log messages when
# each sdminfo document comes in.
if __name__ == '__main__':
    sdminfo_client = SdminfoClient()
    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf on control-C
        logger.info('got SIGINT, exiting')
