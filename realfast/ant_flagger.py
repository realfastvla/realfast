from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open
from future.moves.urllib.parse import urlparse, urlunparse, urlencode
from future.moves.urllib.request import urlopen

import os.path
from lxml import etree, objectify
import numpy as np
import logging
logger = logging.getLogger(__name__)

_install_dir = os.path.abspath(os.path.dirname(__file__))
_xsd_dir = os.path.join(_install_dir, 'xsd')
# TODO: get schema
_antflagger_xsd = os.path.join(_xsd_dir, 'AntFlaggerMessage.xsd')
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
    def _url(self):
        query = '?'
        if self.startTime is not None:
            query += 'startTime='+self.startTime
        if self.endTime is not None:
            query += 'endTime='+self.endTime
        url = 'https://{0}/{1}/{2}/{3}'.format(self.host, 'evla-mcaf-test/dataset',
                                               self.datasetId, 'flags')
        if query:
            url += query
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
            return [flag.attrib for flag in self.response.findall('flag')]
        except AttributeError:
            logger.warn("No ant flags found.")
            return None


def getflags0(datasetId, startTime, endTime):
    """ 
    """

    logger.info("Getting flags for datasetId {0}"
                .format(datasetId))
    antf = ANTFlagger(datasetId, startTime, endTime)
    antf.send()

    return antf.flags


def getflags(datasetId, blarr, startTime=None, endTime=None):
    """ Call antenna flag server for given datasetId and optional
    startTime and endTime.
    blarr is baselines to be flagged (see rfpipe state.blarr),
    which sets structure of flags.
    """

    # set up query to flag server
    host = 'mctest.evla.nrao.edu'
    query = '?'
    if startTime is not None:
        query += 'startTime={0}&'.format(startTime)
    if endTime is not None:
        query += 'endTime={0}'.format(endTime)
    url = 'https://{0}/evla-mcaf-test/dataset/{1}/flags{2}'.format(host,
                                                                   datasetId,
                                                                   query)
    # call server and parse response
    response_xml = urlopen(url).read()
    response = objectify.fromstring(response_xml, parser=_antflagger_parser)

    # find bad ants and baselines
    badants = set(sorted([int(flag.attrib['antennas'].lstrip('ea'))
                          for flag in response.findall('flag')]))

    flags = np.ones(len(blarr), dtype=int)
    for badant in badants:
        flags *= (badant != blarr).prod(axis=1)

    return flags