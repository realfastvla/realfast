from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import os.path
from elasticsearch import Elasticsearch, RequestError
from elasticsearch import helpers
import pickle
import logging
logging.getLogger('elasticsearch').setLevel(30)
logger = logging.getLogger(__name__)
logger.setLevel(10)

# eventually should be updated to search.realfast.io/api with auth
es = Elasticsearch(['go-nrao-nm.aoc.nrao.edu:9200'])


def indexscan_config(config, preferences=None, datasource='vys',
                     indexprefix=''):
    """ Takes scan config and creates dict to push
    to elasticsearch scnas index.
    Optionally pushes rfpipe preferences object as separate doc
    and connects them via the hexdigest name.
    Note index names must end in s and types are derived as singular form.
    datasource is assumed to be 'vys', but 'sim' is an option.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='' for production.
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
        indexprefs(preferences, indexprefix=indexprefix)

    # push scan info with unique id of scanId
    index = indexprefix+'scans'
    res = pushdata(scandict, index=index, Id=config.scanId,
                   command='index')
    if res == 1:
        logger.info('Indexed scan config {0} to {1}'.format(config.scanId,
                                                            index))
    else:
        logger.warn('Scan config not indexed for {0}'.format(config.scanId))


def indexscan_sdm(sdmfile, sdmscan, sdmsubscan, preferences=None,
                  datasource='sdm', indexprefix=''):
    """ Takes sdmfile and sdmscan and pushes to elasticsearch scans index.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='' for production.
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
        indexprefs(preferences, indexprefix=indexprefix)

    # push scan info with unique id of scanId
    index = indexprefix+'scans'
    res = pushdata(scandict, index=index, Id=scanId,
                   command='index')
    if res == 1:
        logger.info('Indexed scan config {0} to {1}'.format(scanId, index))
    else:
        logger.warn('Scan config not indexed for {0}'.format(scanId))


def indexscan_meta(metadata, preferences=None, indexprefix=''):
    """ Takes metadata object and pushes to elasticsearch scans index.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='' for production.
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
        indexprefs(preferences, indexprefix=indexprefix)

    # push scan info with unique id of scanId
    index = indexprefix+'scans'
    res = pushdata(scandict, index=index, Id=metadata.scanId,
                   command='index')
    if res == 1:
        logger.info('Indexed scan config {0} to {1}'
                    .format(metadata.scanId, index))
    else:
        logger.warn('Scan config not indexed for {0}'.format(metadata.scanId))


def indexprefs(preferences, indexprefix=''):
    """ Index preferences with id equal to hash of contents.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='' for production.
    """

    index = indexprefix+'preferences'
    res = pushdata(preferences.ordered, index=index,
                   Id=preferences.name, command='index')
    if res == 1:
        logger.info('Indexed preference {0} to {1}'
                    .format(preferences.name, index))
    else:
        logger.warn('Preferences not indexed for {0}'.format(preferences.name))


def indexcands(candcollection, scanId, tags=None, url_prefix=None,
               indexprefix=''):
    """ Takes candidate collection and pushes to index
    Connects to preferences via hashed name
    scanId is added to associate cand to a give scan.
    Assumes scanId is defined as:
    datasetId dot scanNo dot subscanNo.
    tags is a comma-delimited string used to fill tag field in index.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='' for production.
    """

    if tags is None:
        tags = 'new'

    index = indexprefix+'cands'

    # create new tag string with standard format to fill in blanks
    allowed_tags = ["new", "rfi", "bad", "noise", "needs flagging",
                    "needs review", "interesting", "pulsar", "frb", "mock",
                    "public"]
    tagstr = ','.join([tag if tag in tags.split(',') else '_'
                       for tag in allowed_tags])

    candarr = candcollection.array
    prefs = candcollection.prefs
    candmjd = candcollection.candmjd
    canddm = candcollection.canddm
    canddt = candcollection.canddt

    res = 0
    for i in range(len(candarr)):
        # get features. use .item() to cast to default types
        canddict = dict(list(zip(candarr.dtype.names, candarr[i].item())))

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
        canddict['png_url'] = ''
        if prefs.name:
            canddict['prefsname'] = prefs.name

        if 'snr2' in candarr.dtype.names:
            snr = candarr[i]['snr2']
        elif 'snr1' in candarr.dtype.names:
            snr = candarr[i]['snr1']
        else:
            logger.warn("Neither snr1 nor snr2 in field names. Not pushing.")
            snr = -999

        # create id
        uniqueid = candid(canddict)
        candidate_png = 'cands_{0}.png'.format(uniqueid)
        if snr >= prefs.sigma_plot and os.path.exists(candidate_png):
                logger.debug("Found png {0} and setting cands index field"
                             .format(candidate_png))
                canddict['png_url'] = os.path.join(url_prefix, candidate_png)

        res += pushdata(canddict, index=index,
                        Id=uniqueid, command='index')

    if res >= 1:
        logger.debug('Indexed {0} cands for {1} to {2}'.format(res, scanId,
                                                               index))
    else:
        logger.debug('No cands indexed for {0}'.format(scanId))

    return res


def indexmocks(st, indexprefix=''):
    """ Reads simulated_transient from state and pushes to index
    Mock index must include scanId to reference data that was received.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='' for production.
    """

    if st.prefs.simulated_transient is None:
        return 0

    mocks = st.prefs.simulated_transient
    scanId = st.metadata.scanId
    index = indexprefix+'mocks'

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

        res += pushdata(mockdict, Id=scanId, index=index,
                        command='index')

    if res >= 1:
        logger.debug('Indexed {0} mocks for {1} to {2}'.format(res, scanId,
                                                               index))
    else:
        logger.debug('No mocks indexed for {0}'.format(scanId))

    return res


def indexnoises(noisefile, scanId, indexprefix=''):
    """ Reads noises from noisefile and pushes to index
    scanId is added to associate cand to a give scan.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='' for production.
    """

    index = indexprefix+'noises'
    doc_type = index.rstrip('s')

    noises = []
    with open(noisefile, 'rb') as pkl:
        noises += pickle.load(pkl)

    count = 0
    for noise in noises:
        segment, integration, noiseperbl, zerofrac, imstd = noise
        Id = '{0}.{1}.{2}'.format(scanId, segment, integration)
        if not es.exists(index=index, doc_type=doc_type, id=Id):
            noisedict = {}
            noisedict['scanId'] = str(scanId)
            noisedict['segment'] = int(segment)
            noisedict['integration'] = int(integration)
            noisedict['noiseperbl'] = float(noiseperbl)
            noisedict['zerofrac'] = float(zerofrac)
            noisedict['imstd'] = float(imstd)

            count += pushdata(noisedict, Id=Id, index=index,
                              command='index')

    if count:
        logger.debug('Indexed {0} noises for {1} to {2}'
                     .format(count, scanId, index))
    else:
        logger.debug('No noises indexed for {0}'.format(scanId))

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
                logger.warn('Id={0} already exists in index {1}'
                            .format(Id, index))

    elif command == 'delete':
        if es.exists(index=index, doc_type=doc_type, id=Id):
            res = es.delete(index=index, doc_type=doc_type, id=Id)
        else:
            logger.warn('Id={0} not in index'.format(Id))

    if res:
        return res['_shards']['successful']
    else:
        return res


def candid(data):
    """ Returns id string for given data dict
    Assumes scanId is defined as:
    datasetId dot scanNum dot subscanNum
    """

    return ('{0}_seg{1}-i{2}-dm{3}-dt{4}'
            .format(data['scanId'], data['segment'], data['integration'],
                    data['dmind'], data['dtind']))


def get_ids(index, **kwargs):
    """ Gets Ids from an index
    doc_type derived from index name (one per index)
    Can optionally pass key-value pairs of field-string to search.
    Must match exactly (e.g., "scanId"="test.1.1")
    """

    # only one doc_type per index and its name is derived from index
    doc_type = index.rstrip('s')

    if len(kwargs):
        query = {"query": {"match": kwargs}, "stored_fields": []}
    else:
        query = {"query": {"match_all": {}}, "stored_fields": []}

    res = helpers.scan(es, index=index, doc_type=doc_type, query=query)

    return [hit['_id'] for hit in res]


def update_field(index, field, value, **kwargs):
    """ Replace an index's field with a value.
    Optionally query the index with kwargs.
    Use with caution.
    """

    doc_type = index.rstrip('s')
    if len(kwargs):
        searchquery = {"match": kwargs}
    else:
        searchquery = {"match_all": {}}
    query = {"query": searchquery,
             "script": {"inline": "ctx._source.{0}='{1}'".format(field, value),
                        "lang": "painless"}}

    resp = es.update_by_query(body=query, doc_type=doc_type, index=index)

    response_info = {"total": resp["total"], "updated": resp["updated"],
                     "type": "success"}
    if resp["failures"] != []:
        response_info["type"] = "failure"

    return response_info


def remove_ids(index, **kwargs):
    """ Gets Ids from an index
    doc_type derived from index name (one per index)
    Can optionally pass key-value pairs of field-string to search.
    Must match exactly (e.g., "scanId"="test.1.1")
    """

    if not len(kwargs):
        logger.warn("No query kwargs provided. Will clear all Ids in {0}"
                    .format(index))
        confirm = input("Press any key to confirm removal.")
        if confirm:
            logger.info("Removing...")

    Ids = get_ids(index, **kwargs)
    res = 0
    for Id in Ids:
        res += pushdata({}, index, Id, command='delete')

    logger.info("Removed {0} docs from index {1}".format(res, index))


def reset_indices(indexprefix):
    """ Remove entries from set of indices with a given indexprefix.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='' for production.

    *BE SURE YOU KNOW WHAT YOU ARE DOING*
    """
    logger.warn("Erasing all docs from indices with prefix {0}"
                .format(indexprefix))

    for index in [indexprefix+'noises', indexprefix+'mocks',
                  indexprefix+'cands', indexprefix+'scans']:
        confirm = input("Confirm removal of {0} entries"
                        .format(index))
        res = 0
        if confirm:
            Ids = get_ids(index)
            for Id in Ids:
                res += pushdata({}, index, Id, command='delete')
        logger.info("Removed {0} docs from index {1}".format(res, index))

    # clearing preferences should save just one to keep mapping working
    index = indexprefix+'preferences'
    confirm = input("Confirm removal of {0} entries (saving 1)"
                    .format(index))
    if confirm:
        Ids = get_ids(index)
        res = 0
        for Id in Ids[1:]:
            res += pushdata({}, index, Id, command='delete')
        logger.info("Removed {0} docs from index {1}".format(res, index))
