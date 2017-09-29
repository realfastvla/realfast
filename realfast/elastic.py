from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open
from elasticsearch import Elasticsearch

import logging
logger = logging.getLogger(__name__)

# eventually should be updated to search.realfast.io/api
es = Elasticsearch(['go-nrao-nm.aoc.nrao.edu:9200'])


def indexconfig(config, index='realfast'):
    """ Takes scan config and creates dict to push
    to elasticsearch index
    """

    datadict = {}
    properties = ['scanId', 'datasetId', 'projid', 'ra_deg', 'dec_deg',
                  'scan_intent', 'source', 'startTime', 'stopTime']

    for prop in properties:
        datadict[prop] = getattr(config, prop)

    pushdata(datadict, command='index')


def pushdata(datadict, index='realfast',
             command='index', force=False):
    """ Pushes dict from configuration to index
    Command can be 'index' or 'delete'.
    To update, index with existing key and force=True.
    Assumes "observation" type and id set by scanId.
    """

    assert isinstance(datadict, dict)
    assert 'scanId' in datadict

    doc_type = 'observation'
    scanId = datadict['scanId']

    currentids = getids(index, doc_type)
    logger.info('Pushing scanId {0}'.format(scanId))
    res = None
    
    if command == 'index':
        if force:
            res = es.index(index=index, doc_type=doc_type, id=scanId,
                           body=datadict)
        else:
            if scanId not in currentids:
                res = es.index(index=index, doc_type=doc_type,
                               id=scanId, body=datadict)
            else:
                logger.warn('scanId={0} already exists'
                            .format(scanId))

    elif command == 'delete':
        if scanId in currentids:
            res = es.delete(index=index, doc_type=doc_type, id=scanId)
        else:
            logger.warn('scanId={0} not in index'.format(scanId))

    return res['_shards']['successful']


def getids(index, doc_type):
    """ Gets scanIds from index and returns them as list """

    count = es.count('realfast')['count']
    res = es.search(index=index, doc_type=doc_type, stored_fields=['_id'],
                    body={"query": {"match_all": {}}, "size": count})
    return [hit['_id'] for hit in res['hits']['hits']]
