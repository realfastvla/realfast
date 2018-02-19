from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import os.path
from elasticsearch import Elasticsearch, RequestError
import pickle
import logging
logging.getLogger('elasticsearch').setLevel(30)
logger = logging.getLogger(__name__)

# eventually should be updated to search.realfast.io/api with auth
es = Elasticsearch(['go-nrao-nm.aoc.nrao.edu:9200'])


def indexscan_config(config, preferences=None, datasource='vys'):
    """ Takes scan config and creates dict to push
    to elasticsearch scnas index.
    Optionally pushes rfpipe preferences object as separate doc
    and connects them via the hexdigest name.
    Note index names must end in s and types are derived as singular form.
    datasource is assumed to be 'vys', but 'sim' is an option.
    """

    scandict = {}

    # define dict for scan properties to index
    scandict['datasetId'] = config.datasetId
    scandict['scanId'] = config.scanId
    scandict['projid'] = config.projid
    scandict['scanNo'] = int(config.scanNo)
    scandict['subscanNo'] = int(config.subscanNo)
    scandict['source'] = str(config.source)
    scandict['ra_deg'] = float(config.ra_deg)
    scandict['dec_deg'] = float(config.dec_deg)
    scandict['startTime'] = float(config.startTime)
    scandict['stopTime'] = float(config.stopTime)
    scandict['datasource'] = datasource
    scandict['scan_intent'] = config.scan_intent

    # if preferences provided, it will connect them by a unique name
    if preferences:
        scandict['prefsname'] = preferences.name
        indexprefs(preferences)

    # push scan info with unique id of scanId
    res = pushdata(scandict, index='scans', Id=config.scanId, command='index')
    if res == 1:
        logger.info('Successfully indexed scan config')
    else:
        logger.warn('Scan config not indexed')


def indexscan_sdm(sdmfile, sdmscan, sdmsubscan, preferences=None,
                  datasource='sdm'):
    """ Takes sdmfile and sdmscan and pushes to elasticsearch scans index.
    """

# must include:
#    scanproperties = ['datasetId', 'scanNo', 'subscanNo', 'projid', 'ra_deg',
#                      'dec_deg', 'scan_intent', 'source', 'startTime',
#                      'stopTime']

    import sdmpy
    from numpy import degrees

    datasetId = os.path.basename(sdmfile.rstrip('/'))
    scanId = '{0}.{1}.{2}'.format(datasetId, str(sdmscan), str(sdmsubscan))

    sdm = sdmpy.SDM(sdmfile, use_xsd=False)
    scan = sdm.scan(sdmscan)

    scandict = {}

    # define dict for scan properties to index
    scandict['datasetId'] = datasetId
    scandict['scanId'] = scanId
    scandict['projid'] = 'Unknown'
    scandict['scanNo'] = int(sdmscan)
    scandict['subscanNo'] = int(sdmsubscan)
    scandict['source'] = str(scan.source)
    ra, dec = degrees(scan.coordinates)
    scandict['ra_deg'] = float(ra)
    scandict['dec_deg'] = float(dec)
    scandict['startTime'] = float(scan.startMJD)
    scandict['stopTime'] = float(scan.endMJD)
    scandict['datasource'] = datasource
    scandict['scan_intent'] = ','.join(scan.intents)

    # if preferences provided, it will connect them by a unique name
    if preferences:
        scandict['prefsname'] = preferences.name
        indexprefs(preferences)

    # push scan info with unique id of scanId
    res = pushdata(scandict, index='scans', Id=scanId, command='index')
    if res == 1:
        logger.info('Successfully indexed scan config')
    else:
        logger.warn('Scan config not indexed')


def indexscan_meta(metadata, preferences=None):
    """ Takes metadata object and pushes to elasticsearch scans index.
    """

# must include:
#    scanproperties = ['datasetId', 'scanNo', 'subscanNo', 'projid', 'ra_deg',
#                      'dec_deg', 'scan_intent', 'source', 'startTime',
#                      'stopTime']

    from numpy import degrees

    scandict = {}

    # define dict for scan properties to index
    scandict['datasetId'] = metadata.datasetId
    scandict['scanId'] = metadata.scanId
    scandict['projid'] = 'Unknown'
    scandict['scanNo'] = int(metadata.scan)
    scandict['subscanNo'] = int(metadata.subscan)
    scandict['source'] = metadata.source
    ra, dec = degrees(metadata.radec)
    scandict['ra_deg'] = float(ra)
    scandict['dec_deg'] = float(dec)
    scandict['startTime'] = float(metadata.starttime_mjd)
    scandict['stopTime'] = float(metadata.endtime_mjd)
    scandict['datasource'] = metadata.datasource
    scandict['scan_intent'] = metadata.intent  # assumes ,-delimited string

    # if preferences provided, it will connect them by a unique name
    if preferences:
        scandict['prefsname'] = preferences.name
        indexprefs(preferences)

    # push scan info with unique id of scanId
    res = pushdata(scandict, index='scans', Id=metadata.scanId,
                   command='index')
    if res == 1:
        logger.info('Successfully indexed scan config')
    else:
        logger.warn('Scan config not indexed')


def indexprefs(preferences):
    """
    """

    res = pushdata(preferences.ordered, index='preferences',
                   Id=preferences.name, command='index')
    if res == 1:
        logger.info('Successfully indexed preferences')
    else:
        logger.warn('Preferences not indexed')


def indexcands(candcollection, scanId, tags=None, url_prefix=None):
    """ Takes candidate collection and pushes to index
    Connects to preferences via hashed name
    scanId is added to associate cand to a give scan.
    Assumes scanId is defined as:
    datasetId dot scanNo dot subscanNo.
    tags is a comma-delimited string used to fill tag field in index.
    """

    if tags is None:
        tags = 'new'

    # create new tag string with standard format to fill in blanks
    allowed_tags = ["new", "rfi", "bad", "noise", "needs flagging",
                    "needs review", "interesting", "pulsar", "frb", "mock",
                    "public"]
    tagstr = ','.join(list(map(lambda tag: tag if tag == tags else '_',
                               allowed_tags)))

    candarr = candcollection.array
    prefs = candcollection.prefs
    candmjd = candcollection.candmjd
    canddm = candcollection.canddm
    canddt = candcollection.canddt

    res = 0
    for i in range(len(candarr)):
        # get features. use .item() to cast to default types
        canddict = dict(zip(candarr.dtype.names, candarr[i].item()))

        # fill optional fields
        canddict['scanId'] = scanId
        datasetId, scan, subscan = scanId.rsplit('.', 2)
        canddict['datasetId'] = datasetId
        canddict['scan'] = int(scan)
        canddict['subscan'] = int(subscan)
        canddict['tags'] = tagstr
        canddict['candmjd'] = float(candmjd[i])
        canddict['canddm'] = float(canddm[i])
        canddict['canddt'] = float(canddt[i])
        if prefs.name:
            canddict['prefsname'] = prefs.name

        # create id
        uniqueid = candid(canddict)
        if 'snr2' in candarr.dtype.names:
            snr = candarr[i]['snr2']
        elif 'snr1' in candarr.dtype.names:
            snr = candarr[i]['snr1']
        else:
            logger.warn("Neither snr1 nor snr2 in field names. Not pushing.")
            snr = -999

        if snr >= prefs.sigma_plot:
            # TODO: test for existance of file prior to setting field?
            candidate_png = 'cands_{0}.png'.format(uniqueid)
            canddict['png_url'] = os.path.join(url_prefix, candidate_png)

            res += pushdata(canddict, index='cands',
                            Id=uniqueid, command='index')

    if res >= 1:
        logger.debug('Successfully indexed {0} candidates'.format(res))
    else:
        logger.debug('No candidates indexed')

    return res


def candid(data):
    """ Returns id string for given data dict
    Assumes scanId is defined as:
    datasetId dot scanNum dot subscanNum
    """

    return ('{0}_seg{1}-i{2}-dm{3}-dt{4}'
            .format(data['scanId'], data['segment'], data['integration'],
                    data['dmind'], data['dtind']))


def indexmocks(inprefs, scanId):
    """ Reads simulated_transient from preferences dict and pushes to index
    Mock index must include scanId to reference data that was received.
    scanId is added to associate cand to a give scan.
    """

    if 'simulated_transient' not in inprefs:
        return 0

    mocks = inprefs['simulated_transient']

    res = 0
    for mock in mocks:
        mockdict = {}
        mockdict['scanId'] = scanId
        (seg, i0, dm, dt, amp, l, m) = mock
        mockdict['segment'] = int(seg)
        mockdict['integration'] = int(i0)
        mockdict['dm'] = float(dm)
        mockdict['dt'] = float(dt)
        mockdict['amp'] = float(amp)
        mockdict['l'] = float(l)
        mockdict['m'] = float(m)

        res += pushdata(mockdict, Id=scanId, index='mocks', command='index')

    if res >= 1:
        logger.debug('Successfully indexed {0} mocks'.format(res))
    else:
        logger.debug('No mocks indexed')

    return res


def indexnoises(noisefile, scanId):
    """ Reads noises from noisefile and pushes to index
    scanId is added to associate cand to a give scan.
    """

    noises = []
    with open(noisefile) as pkl:
        noises += pickle.load(pkl)

    count = 0
    for noise in noises:
        segment, integration, noiseperbl, zerofrac, imstd = noise
        Id = '{0}.{1}.{2}'.format(scanId, segment, integration)
        if not es.exists(index='noises', doc_type='noise', id=Id):
            noisedict = {}
            noisedict['scanId'] = str(scanId)
            noisedict['segment'] = int(segment)
            noisedict['integration'] = int(integration)
            noisedict['noiseperbl'] = float(noiseperbl)
            noisedict['zerofrac'] = float(zerofrac)
            noisedict['imstd'] = float(imstd)
            count += pushdata(noisedict, Id=Id, index='noises',
                              command='index')

    if count:
        logger.debug('Successfully indexed {0} noises'.format(count))
    else:
        logger.debug('No noises indexed')

    return count


def pushdata(datadict, index, Id=None, command='index', force=False):
    """ Pushes dict to index, which can be:
    candidates, scans, preferences, or noises
    Assuming one elasticsearch doc_type per index (less the s)
    Id for scan should be scanId, while for preferences should be hexdigest
    Command can be 'index' or 'delete'.
    To update, index with existing key and force=True.
    """

    assert isinstance(datadict, dict)

    # only one doc_type per index and its name is derived from index
    doc_type = index.rstrip('s')

    logger.debug('Pushing to index {0} with Id {1}'.format(index, Id))
    res = 0

    if command == 'index':
        if force:
            res = es.index(index=index, doc_type=doc_type, id=Id,
                           body=datadict)
        else:
            if not es.exists(index=index, doc_type=doc_type, id=Id):
                try:
                    res = es.index(index=index, doc_type=doc_type,
                                   id=Id, body=datadict)
                except RequestError:
                    logger.warn("Id {0} and data {1} not indexed due to request error."
                                .format(Id, datadict))
            else:
                logger.warn('Id={0} already exists'
                            .format(Id))

    elif command == 'delete':
        if es.exists(index=index, doc_type=doc_type, id=Id):
            res = es.delete(index=index, doc_type=doc_type, id=Id)
        else:
            logger.warn('Id={0} not in index'.format(Id))

    if res:
        return res['_shards']['successful']
    else:
        return res


def getids(index):
    """ Gets Ids from an index
    doc_type derived from index name (one per index)
    """

    # only one doc_type per index and its name is derived from index
    doc_type = index.rstrip('s')

    count = es.count(index)['count']
    res = es.search(index=index, doc_type=doc_type, stored_fields=['_id'],
                    body={"query": {"match_all": {}}, "size": count})
    return [hit['_id'] for hit in res['hits']['hits']]
