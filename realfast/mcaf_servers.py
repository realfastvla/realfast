from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open
from future.moves.urllib.parse import urlparse, urlunparse, urlencode
from future.moves.urllib.request import urlopen
from future.moves.urllib.error import HTTPError

import os.path
from lxml import etree, objectify
import json
from astropy import time
import numpy as np
import logging
logger = logging.getLogger(__name__)
logger.setLevel(20)

_install_dir = os.path.abspath(os.path.dirname(__file__))
_xsd_dir = os.path.join(_install_dir, 'xsd')
_sdmbuilder_xsd = os.path.join(_xsd_dir, 'SdmBuilderMessage.xsd')
_sdmbuilder_parser = objectify.makeparser(
        schema=etree.XMLSchema(file=_sdmbuilder_xsd))
_antflagger_xsd = os.path.join(_xsd_dir, 'AntFlaggerMessage.xsd')
_antflagger_parser = objectify.makeparser(
        schema=etree.XMLSchema(file=_antflagger_xsd))

_host = 'mctest.evla.nrao.edu'
_flaghost = 'mchammer.evla.nrao.edu'
_sdmpath = 'sdm-builder/offline'
_antpath = 'evla-mcaf-production/dataset'


class SDMBuilder(object):
    """ Use mcaf to create new SDM from bdf
    """

    _E = objectify.ElementMaker(annotate=False)

    def __init__(self, datasetId=None, outputDatasetId=None,
                 uid=None, dataSize=None,
                 numIntegrations=None, startTime=None, endTime=None,
                 calScanTime=None, annotation={},
                 host=_host, path=_sdmpath):
        self.datasetId = datasetId
        if outputDatasetId is None:
            self.outputDatasetId = 'realfast_{0}_{1}'.format(datasetId,
                                                             uid.rsplit('/')[-1])
        else:
            self.outputDatasetId = outputDatasetId
        self.uid = uid
        self.dataSize = dataSize
        self.numIntegrations = numIntegrations
        self.startTime = startTime
        self.endTime = endTime
        self.calScanTime = calScanTime
        self.annotation = annotation
        self.host = host
        self.path = path

    @property
    def _root(self):
        return self._E.SdmBuilderMessage(
                self._E.datasetId(self.datasetId),
                self._E.outputDatasetId(self.outputDatasetId),
                self._E.bdf(
                    self._E.uid(self.uid),
                    self._E.dataSize(self.dataSize),
                    self._E.numIntegrations(self.numIntegrations),
                    self._E.startTime(repr(self.startTime)),
                    self._E.endTime(repr(self.endTime)),
                    self._E.calScanTime(repr(self.calScanTime)),
                    ),
                self._E.annotation(
                    self._E.sValue(json.dumps(self.annotation)),
                    # annotation attributes:
                    {'issue': 'REALFAST_METADATA',
                        'details': 'Realfast candidate metadata'}
                    ),
                # SdmBuilderMessage attributes:
                {'timestamp': '%.12f' % time.Time.now().mjd,
                    'sender': 'realfast'}
                )

    @property
    def xml(self):
        return etree.tostring(self._root, xml_declaration=True,
                              pretty_print=False, standalone=True)

    @property
    def _url(self):
        query = urlencode({'xml': self.xml})
        url = urlunparse(('https', self.host, self.path, '', query, ''))
        return url

    def send(self):
        response_xml = urlopen(self._url, timeout=60).read()
        self.response = None
        if b'error' in response_xml:
            logger.warn("error in sdmbuilder response xml: {0}"
                        .format(response_xml))
            self.response = response_xml
        else:
            self.response = objectify.fromstring(response_xml,
                                                 parser=_sdmbuilder_parser)

    @property
    def location(self):
        try:
            return str(self.response.result.location)
        except AttributeError:
            logger.warn("No SDM generated.")
            return None


def makesdm(startTime, endTime, datasetId, data, calScanTime=None,
            annotation={}, returnbuilder=False):
    """ Generates call to sdm builder server for a single candidate.
    Generates a unique id for the bdf from the startTime.
    Uses datasetId and data to create call signature to server with:
    (datasetId, dataSize_bytes, nint, startTime_mjd, endTime_mjd)
    Returns location of newly created SDM.
    Data refers to cut out visibilities from startTime to endTime with
    shape of (nint, nbl, nspw, numBin, nchan, npol).
    annotation is a dict that is made into json and attached to SDM.
    output sdm naming convention is "realfast_datasetId_uid",
    where uid is startTime in unix milliseconds (as used for BDF).
    returnbuilder can be used to get SDMBuilder object.
    """

    assert data.ndim == 6, ("data must have 6 dimensions: "
                            "nint, nbl, nspw, numBin, nchan, npol.")

    nint, nbl, nspw, numBin, nchan, npol = data.shape
    dataSize = data.nbytes
    uid = ('uid:///evla/realfastbdf/{0}'
           .format(int(time.Time(startTime, format='mjd').unix*1e3)))
    outputDatasetId = 'realfast_{0}_{1}'.format(datasetId, uid.rsplit('/')[-1])

    logger.info("Building SDM for datasetId {0} and bdf {1} at {2}"
                .format(datasetId, uid, outputDatasetId))
    sdmb = SDMBuilder(datasetId=datasetId, uid=uid, dataSize=dataSize,
                      numIntegrations=nint, startTime=startTime,
                      endTime=endTime, outputDatasetId=outputDatasetId,
                      calScanTime=calScanTime, annotation=annotation)
    try:
        sdmb.send()
        ret = sdmb.location
    except HTTPError:
        logger.warn("HTTPError in SDM builder. xml: {0}"
                    .format(sdmb.xml))
        ret = None

    if returnbuilder:
        return sdmb
    else:
        return ret


def makebdf(startTime, endTime, metadata, data, bdfdir='.'):
    """ Create bdf for candidate that contains data array.
    Data is numpy array of complex64 type spanning start/endTime.
    Should have shape (nint, nbl, nspw, nbin, nchan, npol).
    metadata is a rfpipe.metadata.Metadata object.
    Assumes one bdf per sdm and one sdm per candidate.
    Only supports 8bit samplers and IFid of AC/BD.
    """

    from sdmpy import bdf
    from rfpipe.metadata import Metadata

    assert type(metadata) == Metadata, ("metadata must be "
                                        "of type rfpipe.metadata.Metadata")

    assert data.ndim == 6, ("data must have 6 dimensions: "
                            "nint, nbl, nspw, numBin, nchan, npol.")

    nint, nbl, nspw, numBin, nchan, npol = data.shape

    IFidspwnum = [spw.split('-') for (spw, freq) in metadata.spworder]
    spws = [bdf.BDFSpectralWindow(None, numBin=numBin, numSpectralPoint=nchan,
                                  sw=int(swnum)+1,   # casting from 0->1 based
                                  swbb='{0}_8BIT'.format(IFid),
                                  npol=npol) for (IFid, swnum) in IFidspwnum]
    # TODO: confirm that sw is 1 based for a proper SDM
    # TODO: confirm that metadata spworder is 0 based

    assert nspw == len(spws), ('Expected one spw in metadata.spworder per spw '
                               'in data array.')
    assert os.path.isdir(bdfdir), 'bdfdir does not exist'

    uid = ('uid:///evla/realfastbdf/{0}'
           .format(int(time.Time(startTime, format='mjd').unix*1e3)))
    w = bdf.BDFWriter(bdfdir, start_mjd=startTime, uid=uid,
                      num_antenna=metadata.nants_orig, spws=spws, scan_idx=1,
                      corr_mode='c')

    dat = {}
    w.write_header()
    for i in range(nint):
        dat['crossData'] = data[i]
        ts = startTime + i*metadata.inttime/86400.
        w.write_integration(mjd=ts, interval=metadata.inttime, data=dat)
    w.close()


def getblflags(datasetId, blarr, startTime=None, endTime=None):
    """ Call antenna flag server for given datasetId and return
    flags per baseline. Optional input are startTime and endTime.
    blarr is array of baselines to be flagged (see rfpipe state.blarr)
    that defines structure of returned flag array.
    """

    # set up query to flag server
    query = '?'
    if startTime is not None:
        query += 'startTime={0}&'.format(startTime)
    if endTime is not None:
        query += 'endTime={0}'.format(endTime)
    url = 'https://{0}/{1}/{2}/flags{3}'.format(_flaghost, _antpath, datasetId,
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
