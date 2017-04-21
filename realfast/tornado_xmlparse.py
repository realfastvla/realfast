import argparse
from lxml import etree
from tornado import gen, ioloop, tcpserver
from tornado.iostream import IOStream, StreamClosedError
import sdminfoxml_parser, obsxml_parser, antxml_parser


# parse name of server(s)
parser = argparse.ArgumentParser()
parser.add_argument("name", type=str, help="Name of document to expect (xml, sdminfo, obs, ant, all)")
parser.add_argument("--port", default=0, type=int, help="Optional port number for xml listening")
opts = parser.parse_args()
docname = opts.name.lower()
port = opts.port

# standard CBE addresses and ports
sdminfoaddress = '239.192.5.2'
sdminfoport = 55002 
obsaddress = '239.192.3.2'
obsport = 53001
antaddress = '239.192.3.1'
antport = 53000

assert docname in ["xml", "sdminfo", "obs", "ant", "all"]

class XMLServer(tcpserver.TCPServer):

    @gen.coroutine
    def handle_stream(self, stream, address):
        try:
            msg = yield stream.read_until_close()
            print("Recieved msg of length {0}".format(len(msg)))

            root = etree.fromstring(msg)
            for child in root:
                print(child.tag, child.attrib)

        except StreamClosedError:
            print("Error connecting")
            yield gen.sleep(5)


class SDMinfoServer(tcpserver.TCPServer):

    @property
    def address:
        return sdminfoaddress

    @gen.coroutine
    def handle_stream(self, stream, address):
        try:
            msg = yield stream.read_until_close()
            print("Recieved msg of length {0}".format(len(msg)))

            sdminfo = sdminfoxml_parser.parseString(msg)
            print('Parsed sdminfo: {0}, {1}'.format(sdminfo.ScanNumber, sdminfo.scanIntents))

        except StreamClosedError:
            print("Error connecting")
            yield gen.sleep(5)


class ObsServer(tcpserver.TCPServer):

    @property
    def address:
        return obsaddress

    @gen.coroutine
    def handle_stream(self, stream, address):
        try:
            msg = yield stream.read_until_close()
            print("Recieved msg of length {0}".format(len(msg)))

            obsinfo = obsxml_parser.parseString(msg)
            print('Parsed obsinfo: {0}, {1}'.format('obs test', 'obs test'))

        except StreamClosedError:
            print("Error connecting")
            yield gen.sleep(5)


class AntServer(tcpserver.TCPServer):

    @property
    def address:
        return antaddress

    @gen.coroutine
    def handle_stream(self, stream, address):
        try:
            msg = yield stream.read_until_close()
            print("Recieved msg of length {0}".format(len(msg)))

            antinfo = antxml_parser.parseString(msg)
            print('Parsed antinfo: {0}, {1}'.format('ant test', 'ant test'))

        except StreamClosedError:
            print("Error connecting")
            yield gen.sleep(5)


if __name__ == '__main__':
    servers = []

    if docname in ['xml', 'all']:
        address = 'localhost'

        server = XMLServer()
        server.listen(port, address=address)
        servers.append(server)
        print('Started XMLServer at {0}:{1}'.format(server.address, port))

    if docname in ['sdminfo', 'all']:
        if not port: port = sdminfoport

        server = SDMinfoServer()
        server.listen(port, address=server.address)
        servers.append(server)
        print('Started SDMinfoServer at {0}:{1}'.format(server.address, port))

    if docname in ['obs', 'all']:
        if not port: port = obsport

        server = ObsServer()
        server.listen(port, address=server.address)
        servers.append(server)
        print('Started ObsServer at {0}:{1}'.format(server.address, port))

    if docname in ['antinfo', 'all']:
        if not port: port = antport

        server = AntServer()
        server.listen(port, address=server.address)
        servers.append(server)
        print('Started AntServer at {0}:{1}'.format(server.address, port))

    io_loop = ioloop.IOLoop.current()
    io_loop.start()
