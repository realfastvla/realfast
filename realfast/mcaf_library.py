import os
import struct
import logging
import asyncore, socket
import sdminfoxml_parser, obsxml_parser, antxml_parser, vcixml_parser

logger = logging.getLogger(__name__)

# standard CBE addresses and ports
sdminfoaddress = '239.192.5.2'
sdminfoport = 55002 
obsaddress = '239.192.3.2'
obsport = 53001
antaddress = '239.192.3.1'
antport = 53000
vciaddress = '239.192.3.1'
vciport = 53000


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


class SDMInfoClient(McastClient):
    """Receives sdminfo XML, which is broadcast when the BDF is available.

    If the controller input is given, the
    controller.add_sdminfo(sdminfo) method will be called for every
    document received. Controller is defined as a class in the main
    controller script, and runs job launching.
    """

    def __init__(self, controller=None):
        McastClient.__init__(self, sdminfoaddress, sdminfoport, 'sdminfo')
        self.controller = controller
        logger.info('SDMInfo client initialized')

    def parse(self):
        sdminfoxml = sdminfoxml_parser.parseString(self.read)
        logger.info("Read sdminfo doc")
        if self.controller is not None:
            self.controller.add_sdminfo(sdminfoxml)


class VCIClient(McastClient):
    """Receives VCI XML.
    
    If the controller input is given, the controller.add_vci(vci) method will
    be called for every document received.
    """

    def __init__(self,controller=None):
        McastClient.__init__(self, vciaddress, vciport, 'vci')
        self.controller = controller
        logger.info('VCI client initialized')

    def parse(self):
        vcixml = vcixml_parser.parseString(self.read)
        logging.info("Read vci doc")
        if self.controller is not None:
            self.controller.add_vci(vcixml)


class ObsClient(McastClient):
    """Receives obs XML.

    If the controller input is given, the
    controller.add_obs(obs) method will be called for every
    document received. Controller is defined as a class in the main
    controller script, and runs job launching.
    """

    def __init__(self, controller=None):
        McastClient.__init__(self, obsaddress, obsport, 'obs')
        self.controller = controller
        logger.info('obs client initialized')

    def parse(self):
        obsxml = obsxml_parser.parseString(self.read)
        logger.info("Read obs doc")
        if self.controller is not None:
            self.controller.add_obs(obsxml)


class AntClient(McastClient):
    """Receives ant XML.

    If the controller input is given, the
    controller.add_ant(ant) method will be called for every
    document received. Controller is defined as a class in the main
    controller script, and runs job launching.
    """

    def __init__(self, controller=None):
        McastClient.__init__(self, antaddress, antport, 'ant')
        self.controller = controller
        logger.info('Ant client initialized')

    def parse(self):
        antxml = antxml_parser.parseString(self.read)
        logger.info("Read ant doc")
        if self.controller is not None:
            self.controller.add_ant(antxml)


# This is how comms would be used in a program.  Note that no controller
# is passed, so the only action taken here is to print log messages when
# each sdminfo document comes in.
if __name__ == '__main__':
    sdminfo_client = SDMInfoClient()
    obs_client = ObsClient()
    vci_client = VCIClient()
    ant_client = AntClient()

    try:
        asyncore.loop()
    except KeyboardInterrupt:
        # Just exit without the trace barf on control-C
        logger.info('got SIGINT, exiting')
