import argparse
from lxml import etree
from tornado import gen, ioloop, tcpserver
from tornado.iostream import IOStream, StreamClosedError
import sdminfoxml_parser, obsxml_parser, antxml_parser


# parse name of server(s)
parser = argparse.ArgumentParser()
parser.add_argument("--port", default=0, type=int, help="Optional port number for xml listening")
opts = parser.parse_args()
port = opts.port


# standard CBE addresses and ports
sdminfoaddress = '239.192.5.2'
sdminfoport = 55002 
obsaddress = '239.192.3.2'
obsport = 53001
antaddress = '239.192.3.1'
antport = 53000


class XMLServer(tcpserver.TCPServer):

    def __init__(self, address=None, port=None):
        super(self.__class__, self).__init__()
        self.address = address
        self.port = port


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

if __name__ == '__main__':

    server = XMLServer(address='localhost', port=port)
    server.listen(server.port, address=server.address)
    servers.append(server)

    io_loop = ioloop.IOLoop.current()
    io_loop.start()

    print('Started XMLServer at {0}:{1}'.format(server.address, server.port))
