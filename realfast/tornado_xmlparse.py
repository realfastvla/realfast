import argparse
from lxml import etree
from tornado import gen, ioloop, tcpserver
from tornado.iostream import IOStream, StreamClosedError
import sdminfoxml_parser, obsxml_parser, antxml_parser, eopxml_parser

parser = argparse.ArgumentParser()
parser.add_argument("name", type=str, help="Name of document to expect (xml, sdminfo, obs, ant, eop)")
parser.add_argument("--port", default=0, type=int, help="Optional port number")
opts = parser.parse_args()
docname = opts.name.lower()
port = opts.port

assert docname in ["xml", "sdminfo", "obs", "ant", "eop"]

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


class EopServer(tcpserver.TCPServer):

    @gen.coroutine
    def handle_stream(self, stream, address):
        try:
            msg = yield stream.read_until_close()
            print("Recieved msg of length {0}".format(len(msg)))

            eopinfo = eopxml_parser.parseString(msg)
            print('Parsed eopinfo: {0}, {1}'.format('eop test', 'eop test'))

        except StreamClosedError:
            print("Error connecting")
            yield gen.sleep(5)


if __name__ == '__main__':
    if docname == 'xml':
        address = 'localhost'
        server = XMLServer()
        server.listen(port)
    elif docname == 'sdminfo':
        address = '239.192.5.2'
        if not port: port = 55002 
        server = SDMinfoServer()
        server.listen(port, address=address)
    elif docname == 'obs':
        address = '239.192.3.2'
        if not port: port = 53001
        server = ObsServer()
        server.listen(port, address=address)
    elif docname == 'antinfo':
        address = '239.192.3.1'
        if not port: port = 53000
        server = AntServer()
        server.listen(port, address=address)
    else:
        print('No such message name')

    print('Listening on {0}:{1}'.format(address, port))
    io_loop = ioloop.IOLoop.current()
    io_loop.start()

# list of ip:ports
#localhost:4444 for playing with xml docs
#239.192.3.1:53000 for AntennaPropertiesTable documents
#239.192.3.2:53001 for Observation documents
#239.192.5.2,55002 for sdminfo documents


