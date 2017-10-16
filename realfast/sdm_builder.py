import os.path
import urllib, urlparse
from lxml import etree, objectify
import astropy.time

_install_dir = os.path.abspath(os.path.dirname(__file__))
_xsd_dir = os.path.join(_install_dir, 'xsd')
_sdmbuilder_xsd = os.path.join(_xsd_dir, 'SdmBuilderMessage.xsd')
_sdmbuilder_parser = objectify.makeparser(
        schema=etree.XMLSchema(file=_sdmbuilder_xsd)
        )

class SDMBuilder(object):
    _E = objectify.ElementMaker(annotate=False)

    def __init__(self, datasetId=None, uid=None, dataSize=None,
            numIntegrations=None, startTime=None, endTime=None,
            host='mctest.evla.nrao.edu', path='sdm-builder/offline'):
        self.datasetId = datasetId
        self.uid = uid
        self.dataSize = dataSize
        self.numIntegrations = numIntegrations
        self.startTime = startTime
        self.endTime = endTime
        self.host = host
        self.path = path

    @property
    def _root(self):
        return self._E.SdmBuilderMessage(
                self._E.datasetId(self.datasetId),
                self._E.bdf(
                    self._E.uid(self.uid),
                    self._E.dataSize(self.dataSize),
                    self._E.numIntegrations(self.numIntegrations),
                    self._E.startTime(self.startTime),
                    self._E.endTime(self.endTime),
                    ),
                # SdmBuilderMessage attributes:
                {'timestamp': '%.12f' % astropy.time.Time.now().mjd,
                    'sender': 'realfast'}
                )

    @property
    def xml(self):
        return etree.tostring(self._root, xml_declaration=True,
                pretty_print=False, standalone=True)

    @property 
    def _url(self):
        query = urllib.urlencode({'xml': self.xml})
        url = urlparse.urlunparse(('https',self.host,self.path,'',query,''))
        return url

    def send(self):
        response_xml = urllib.urlopen(self._url).read()
        # This will raise an exception if the result is not in the 
        # expected XML format, but it does not currently check for
        # an 'error' response.  TODO what to do?
        self.response = objectify.fromstring(response_xml,
                parser=_sdmbuilder_parser)

    @property
    def location(self):
        try:
            return str(self.response.result.location)
        except AttributeError:
            return None

