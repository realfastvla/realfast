from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open
from future.moves.urllib.parse import urlparse, urlunparse, urlencode
from future.moves.urllib.request import urlopen

import os.path
from lxml import etree, objectify
import logging
logger = logging.getLogger(__name__)

_install_dir = os.path.abspath(os.path.dirname(__file__))
_xsd_dir = os.path.join(_install_dir, 'xsd')
# TODO: get schema
_antflagger_xsd = os.path.join(_xsd_dir, 'schema.xsd')
_antflagger_parser = objectify.makeparser(
        schema=etree.XMLSchema(file=_antflagger_xsd))

_host = 'mctest.evla.nrao.edu'


class ANTFlagger(object):
    """ Use mcaf to get online antenna flags """

    _E = objectify.ElementMaker(annotate=False)

    def __init__(self, datasetId=None, startTime=None, endTime=None,
                 host=_host):
        self.datasetId = datasetId
        self.startTime = startTime
        self.endTime = endTime
        self.host = host

    @property
    def _root(self):
        return self._E.ANTFlaggerMessage(
                self._E.datasetId(self.datasetId),
                self._E.startTime(repr(self.startTime)),
                self._E.endTime(repr(self.endTime)))
                # TODO: find if message attributes needed
#                {'timestamp': '%.12f' % time.Time.now().mjd,
#                    'sender': 'realfast'}
#                )

    @property
    def xml(self):
        return etree.tostring(self._root, xml_declaration=True,
                              pretty_print=False, standalone=True)

    @property
    def _url(self):
        query = urlencode({'xml': self.xml})
        url = urlunparse(('https', self.host, 'dataset', '', query, '', 'flags'))
        return url

    def send(self):
        response_xml = urlopen(self._url).read()
        if b'error' in response_xml:
            self.response = None
        else:
            self.response = objectify.fromstring(response_xml,
                                                 parser=_antflagger_parser)

    @property
    def flags(self):
        try:
            return str(self.response.result.flags)
        except AttributeError:
            logger.warn("No ant flags found.")
            return None


def getflags(datasetId, startTime, endTime):
    """ 
    """

    logger.info("Getting flags for datasetId {0}"
                .format(datasetId))
    antf = ANTFlagger(datasetId, startTime, endTime)
    antf.send()

    return antf.flags
