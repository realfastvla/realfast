from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open
from elasticsearch import Elasticsearch

import pickle
import logging
logger = logging.getLogger(__name__)

# eventually should be updated to search.realfast.io/api
es = Elasticsearch(['go-nrao-nm.aoc.nrao.edu:9200'])


def indexscan(config, preferences=None):
    """ Takes scan config and creates dict to push
    to elasticsearch index.
    Optionally pushes rfpipe preferences object as separate doc
    and connects them via the hexdigest name.
    """

    scandict = {}
    scanproperties = ['datasetId', 'scanNo', 'subscanNo', 'projid', 'ra_deg',
                      'dec_deg', 'scan_intent', 'source', 'startTime',
                      'stopTime']

    # define dict for scan properties to index
    for prop in scanproperties:
        scandict[prop] = getattr(config, prop)

    # if preferences provided, it will connect them by a unique name
    if preferences:
        scandict['preferences'] = preferences.name

    # push scan info
    res = pushdata(scandict, index='scans', Id=config.scanId, command='index')
    if res == 1:
        logger.info('Successfully indexed scan config')
    else:
        logger.warn('Scan config not indexed')

    # push preferences
    res = pushdata(preferences.ordered, index='preferences',
                   Id=preferences.name, command='index')
    if res == 1:
        logger.info('Successfully indexed preferences')
    else:
        logger.warn('Preferences not indexed')


def indexcands(candsfile, scanId):
    """ Reads candidates from candsfile and pushes to index
    Optionally adds preferences connection via hashed name
    scanId is added to associate cand to a give scan.
    """

    with open(candsfile) as pkl:
        cand = pickle.load(pkl)

        canddict = {}

        canddict['scanId'] = scanId

        res = pushdata(canddict, index='cands',
                       Id=candict[...], command='index')

    if res >= 1:
        logger.debug('Successfully indexed {0} candidates'.format(res))
    else:
        logger.debug('No candidates indexed')

    return res


def indexmocks(mockfile, scanId):
    """ Reads mocks from mockfile and pushes to index
    Mock index must include scanId to reference data that was received.
    scanId is added to associate cand to a give scan.
    """

    raise NotImplementedError

    mocks = None  # ** TODO

    res = 0
    for mock in mocks:
        mockdict = {}
        mockdict['scanId'] = scanId

        res += pushdata(mockdict, index='mocks', command='index')

    if res >= 1:
        logger.debug('Successfully indexed {0} mocks'.format(res))
    else:
        logger.debug('No mocks indexed')

    return res


def indexnoises(noisefile, scanId):
    """ Reads noises from noisefile and pushes to index
    scanId is added to associate cand to a give scan.
    """

    raise NotImplementedError

    noises = None

    res = 0
    for noise in noises:
        noisedict = {}
        noisedict['scanId'] = scanId
        res += pushdata(noisedict, index='noises', command='index')

    if res >= 1:
        logger.debug('Successfully indexed {0} noises'.format(res))
    else:
        logger.debug('No noises indexed')

    return res


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

    currentids = getids(index)
    logger.debug('Pushing to index {0} with Id {1}'.format(index, Id))
    res = None

    if command == 'index':
        if force:
            res = es.index(index=index, doc_type=doc_type, id=Id,
                           body=datadict)
        else:
            if Id not in currentids:
                res = es.index(index=index, doc_type=doc_type,
                               id=Id, body=datadict)
            else:
                logger.warn('Id={0} already exists'
                            .format(Id))

    elif command == 'delete':
        if Id in currentids:
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

    count = es.count('realfast')['count']
    res = es.search(index=index, doc_type=doc_type, stored_fields=['_id'],
                    body={"query": {"match_all": {}}, "size": count})
    return [hit['_id'] for hit in res['hits']['hits']]
