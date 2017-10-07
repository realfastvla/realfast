from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import pickle
import os.path
from elasticsearch import Elasticsearch
import logging
logger = logging.getLogger(__name__)

# eventually should be updated to search.realfast.io/api
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
    scanproperties = ['datasetId', 'scanNo', 'subscanNo', 'projid', 'ra_deg',
                      'dec_deg', 'scan_intent', 'source', 'startTime',
                      'stopTime']

    # define dict for scan properties to index
    for prop in scanproperties:
        scandict[prop] = getattr(config, prop)
    scandict['datasource'] = datasource

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


def indexscan_sdm(scanId, preferences=None, datasource='sdm'):
    """ Takes sdmfile and sdmscan and pushes to elasticsearch scans index.
    """

# must include:
#    scanproperties = ['datasetId', 'scanNo', 'subscanNo', 'projid', 'ra_deg',
#                      'dec_deg', 'scan_intent', 'source', 'startTime',
#                      'stopTime']

    import sdmpy
    from numpy import degrees

    datasetId, sdmscan, sdmsubscan = scanId.rsplit('.', 2)
    sdm = sdmpy.SDM(datasetId)
    scan = sdm.scan(sdmscan)

    scandict = {}

    # define dict for scan properties to index
    scandict['datasetId'] = datasetId
    scandict['projid'] = 'Unknown'
    scandict['scanNo'] = sdmscan
    scandict['subscanNo'] = sdmsubscan
    scandict['source'] = str(scan.source)
    ra, dec = degrees(degrees(scan.coordinates))
    scandict['ra_deg'] = ra
    scandict['dec_deg'] = dec
    scandict['startTime'] = str(scan.startMJD)
    scandict['stopTime'] = str(scan.endMJD)
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


def indexprefs(preferences):
    """
    """

    res = pushdata(preferences.ordered, index='preferences',
                   Id=preferences.name, command='index')
    if res == 1:
        logger.info('Successfully indexed preferences')
    else:
        logger.warn('Preferences not indexed')


def indexcands(candsfile, scanId, prefsname=None, withplots=True,
               tags=None):
    """ Reads candidates from candsfile and pushes to index
    Optionally adds preferences connection via hashed name
    scanId is added to associate cand to a give scan.
    Assumes scanId is defined as:
    datasetId (a.k.a. metadata.filename) dot scanNo dot subscanNo.
    withplots specifies that only candidates with plots are indexed.
    tags is a comma-delimited string used to fill tag field in index.
    """

    if tags is None:
        tags = 'new'

    res = 0
    with open(candsfile) as pkl:
        while True:  # step through all possible segments
            try:
                cands = pickle.load(pkl)
                for cand in cands.df.itertuples():
                    # get features
                    canddict = dict([(col, cand.__dict__[col])
                                    for col in cands.df.columns])

                    # fill optional fields
                    canddict['scanId'] = scanId
                    datasetId, scan, subscan = scanId.rsplit('.', 2)
                    canddict['datasetId'] = datasetId
                    canddict['scan'] = scan
                    canddict['subscan'] = subscan
                    canddict['tags'] = tags
                    if prefsname:
                        canddict['prefsname'] = prefsname

                    # create id
                    uniqueid = candid(canddict)
                    candidate_png = 'cands_{0}.png'.format(uniqueid)
                    if os.path.exists(candidate_png):  # set if png exists
                        canddict['candidate_png'] = candidate_png

                    if withplots:
                        if os.path.exists(candidate_png):
                            res += pushdata(canddict, index='cands',
                                            Id=uniqueid, command='index')
                        else:
                            logger.info("No plot {0} found"
                                        .format(candidate_png))
                    else:
                        res += pushdata(canddict, index='cands',
                                        Id=uniqueid, command='index')
            except EOFError:
                break

    if res >= 1:
        logger.debug('Successfully indexed {0} candidates'.format(res))
    else:
        logger.debug('No candidates indexed')

    return res


def candid(data):
    """ Returns id string for given data dict
    Assumes scanId is defined as:
    datasetId (a.k.a. metadata.filename) dot scanNum dot subscanNum
    ** TODO: maybe allow scan to be defined as scan.subscan?
    """

    return ('{0}_sc{1}-seg{2}-i{3}-dm{4}-dt{5}'
            .format(data['datasetId'], data['scan'], data['segment'],
                    data['integration'], data['dmind'], data['dtind']))


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
    res = 0

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

    count = es.count(index)['count']
    res = es.search(index=index, doc_type=doc_type, stored_fields=['_id'],
                    body={"query": {"match_all": {}}, "size": count})
    return [hit['_id'] for hit in res['hits']['hits']]
