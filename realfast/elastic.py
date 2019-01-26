from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import os.path
from elasticsearch import Elasticsearch, RequestError, TransportError, helpers
from urllib3.connection import ConnectionError, NewConnectionError
from rfpipe.candidates import calc_cluster_rank
from rfpipe.metadata import make_metadata
from realfast import heuristics
import pickle
import subprocess
import logging
from numpy import degrees
logging.getLogger('elasticsearch').setLevel(30)
logger = logging.getLogger(__name__)
logger.setLevel(10)

# eventually should be updated to search.realfast.io/api with auth
es = Elasticsearch(['realfast.nrao.edu:9200'])


###
# Indexing stuff
###

def indexscan(config=None, inmeta=None, sdmfile=None, sdmscan=None,
              sdmsubscan=1, bdfdir=None, preferences=None, datasource=None,
              indexprefix='new'):
    """ Index properties of scan.
    Uses data source (config, sdm, etc.) to define metadata object.

    """

    meta = make_metadata(inmeta=inmeta, config=config, sdmfile=sdmfile,
                         sdmscan=sdmscan, bdfdir=bdfdir)

    if meta.datasource is None:
        if datasource is not None:
            meta.datasource = datasource
        elif config is not None:
            meta.datasource = 'vys'
        elif (sdmfile is not None) and (sdmscan is not None):
            meta.datasource = 'sdm'
        else:
            logger.warn("Could not determine datasource for indexing.")

    # define dict for scan properties to index
    scandict = {}
    scandict['datasetId'] = meta.datasetId
    scandict['scanId'] = meta.scanId
#    scandict['projid'] = 'Unknown'
    scandict['scanNo'] = int(meta.scan)
    scandict['subscanNo'] = int(meta.subscan)
    scandict['source'] = meta.source
    ra, dec = degrees(meta.radec)
    scandict['ra_deg'] = float(ra)
    scandict['dec_deg'] = float(dec)
    scandict['startTime'] = float(meta.starttime_mjd)
    scandict['stopTime'] = float(meta.endtime_mjd)
    scandict['datasource'] = meta.datasource
    scandict['scan_intent'] = meta.intent  # assumes ,-delimited string
    scandict['inttime'] = meta.inttime
    band = heuristics.reffreq_to_band(meta.spw_reffreq)
    scandict['band'] = band

    # if preferences provided, it will connect them by a unique name
    if preferences:
        scandict['prefsname'] = preferences.name
        scandict['searchtype'] = preferences.searchtype
        scandict['fftmode'] = preferences.fftmode

    # push scan info with unique id of scanId
    index = indexprefix+'scans'
    res = pushdata(scandict, index=index, Id=meta.scanId,
                   command='index')
    if res == 1:
        logger.info('Indexed scanId {0} to {1}'
                    .format(meta.scanId, index))
    else:
        logger.warn('Scan config not indexed for {0}'.format(meta.scanId))

    if preferences:
        indexprefs(preferences, indexprefix=indexprefix)


def indexscanstatus(scanId, nsegment=None, pending=None, finished=None,
                    errors=None, indexprefix='new'):
    """ Update status fields for scanId
    """

    res = 0
    tried = 0
    if nsegment is not None:
        tried += 1
        res += update_field(index=indexprefix+'scans', Id=scanId,
                            field='nsegment', value=int(nsegment))
    if pending is not None:
        tried += 1
        res += update_field(index=indexprefix+'scans', Id=scanId,
                            field='pending', value=int(pending))
    if finished is not None:
        tried += 1
        res += update_field(index=indexprefix+'scans', Id=scanId,
                            field='finished', value=int(finished))
    if errors is not None:
        tried += 1
        res += update_field(index=indexprefix+'scans', Id=scanId,
                            field='errors', value=int(errors))

    logger.debug("Updated {0}/{1} fields with processing status for {2}"
                 .format(res, tried, scanId))


def indexprefs(preferences, indexprefix='new'):
    """ Index preferences with id equal to hash of contents.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='new' for production.
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
               indexprefix='new'):
    """ Takes candidate collection and pushes to index
    Connects to preferences via hashed name
    scanId is added to associate cand to a give scan.
    Assumes scanId is defined as:
    datasetId dot scanNo dot subscanNo.
    tags is a comma-delimited string used to fill tag field in index.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='new' for production.
    """

    if tags is None:
        tags = ''

    index = indexprefix+'cands'

    # create new tag string with standard format to fill in blanks
    allowed_tags = ["rfi", "bad", "noise", "interesting", "astrophysical",
                    "mock"]
    tagstr = ','.join([tag for tag in tags.split(',') if tag in allowed_tags])

    candarr = candcollection.array
    prefs = candcollection.prefs
    candmjd = candcollection.candmjd
    canddm = candcollection.canddm
    canddt = candcollection.canddt
    cluster = candcollection.cluster
    clustersize = candcollection.clustersize
    snrtot = candcollection.snrtot

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
        canddict['tagcount'] = 0
        canddict['candmjd'] = float(candmjd[i])
        canddict['canddm'] = float(canddm[i])
        canddict['canddt'] = float(canddt[i])
        canddict['cluster'] = int(cluster[i])
        canddict['clustersize'] = int(clustersize[i])
        canddict['snrtot'] = float(snrtot[i])
        canddict['png_url'] = ''
        if prefs.name:
            canddict['prefsname'] = prefs.name

        # create id
        uniqueid = candid(canddict)
        candidate_png = 'cands_{0}.png'.format(uniqueid)
        canddict['png_url'] = os.path.join(url_prefix, indexprefix, candidate_png)

#        assert os.path.exists(os.path.join(prefs.workdir, candidate_png)), "Expected png {0} for candidate.".format(candidate_png)
        res += pushdata(canddict, index=index,
                        Id=uniqueid, command='index')

    if res >= 1:
        logger.debug('Indexed {0} cands for {1} to {2}'.format(res, scanId,
                                                               index))
    else:
        logger.debug('No cands indexed for {0}'.format(scanId))

    return res


def indexmock(scanId, mocks, indexprefix='new'):
    """ Takes simulated_transient as used in state and pushes to index.
    Assumes 1 mock in list for now.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='new' for production.
    """

    if len(mocks[0]) != 7:
        return 0

    index = indexprefix+'mocks'

    mockdict = {}
    mockdict['scanId'] = scanId
    (seg, i0, dm, dt, amp, l, m) = mocks[0]  # assume 1 mock
    # TODO: support possible ampslope
    mockdict['segment'] = int(seg)
    mockdict['integration'] = int(i0)
    mockdict['dm'] = float(dm)
    mockdict['dt'] = float(dt)
    mockdict['amp'] = float(amp)
    mockdict['l'] = float(l)
    mockdict['m'] = float(m)

    res = pushdata(mockdict, Id=scanId, index=index,
                   command='index')

    if res >= 1:
        logger.info('Indexed {0} mocks for {1} to {2}'.format(res, scanId,
                                                               index))
    else:
        logger.info('No mocks indexed for {0}'.format(scanId))

    return res


def indexnoises(noisefile, scanId, indexprefix='new'):
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
        logger.info('Indexed {0} noises for {1} to {2}'
                     .format(count, scanId, index))
    else:
        logger.info('No noises indexed for {0}'.format(scanId))

    return count


###
# Managing elasticsearch documents
###

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

    try:
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
    except (ConnectionError, NewConnectionError):
        logger.warn("ConnectionError during push to index. Elasticsearch down?")


def candid(data):
    """ Returns id string for given data dict
    Assumes scanId is defined as:
    datasetId dot scanNum dot subscanNum
    """

    return ('{0}_seg{1}-i{2}-dm{3}-dt{4}'
            .format(data['scanId'], data['segment'], data['integration'],
                    data['dmind'], data['dtind']))


def update_field(index, field, value, Id=None, **kwargs):
    """ Replace an index's field with a value.
    Option to work on single Id or query the index with kwargs.
    Use with caution.
    """

    doc_type = index.rstrip('s')

    if Id is None:
        query = {"script": {"inline": "ctx._source.{0}='{1}'".format(field, value),
                            "lang": "painless"}}
        query['retry_on_conflct'] = 2
        if len(kwargs):
            searchquery = {"match": kwargs}
        else:
            searchquery = {"match_all": {}}

        query["query"] = searchquery

        resp = es.update_by_query(body=query, doc_type=doc_type, index=index,
                                  conflicts="proceed")
    else:
        query = {"doc": {field: value}}
        resp = es.update(id=Id, body=query, doc_type=doc_type, index=index)

    return resp['_shards']['successful']


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


def get_ids(index, **kwargs):
    """ Gets Ids from an index
    doc_type derived from index name (one per index)
    Can optionally pass key-value pairs of field-string to search.
    Must match exactly (e.g., "scanId"="test.1.1")
    """

    # only one doc_type per index and its name is derived from index
    doc_type = index.rstrip('s')
    if 'field' in kwargs:
        field = kwargs.pop('field')
    else:
        field = 'false'

    if len(kwargs):
        query = {"query": {"match": kwargs}, "_source": field}
    else:
        query = {"query": {"match_all": {}}, "_source": field}

    res = helpers.scan(es, index=index, doc_type=doc_type, query=query)

    if field == 'false':
        return [hit['_id'] for hit in res]
    else:
        return [(hit['_id'], hit['_source'][field]) for hit in res]


def get_doc(index, Id):
    """ Get Id from index
    """

    doc_type = index.rstrip('s')
    doc = es.get(index=index, doc_type=doc_type, id=Id)
    return doc


###
# Migrating docs from 'new' to 'final' after tagging
###

def move_docs(indexprefix1='new', indexprefix2='final',
              consensustype='majority', nop=3, newtags=None):
    """ Given candids, copies relevant docs from indexprefix1 to indexprefix2.
    newtags will append to the new "tags" field for all moved candidates.
    Default tags field will contain the user consensus tag.
    """

    consensus = get_consensus(indexprefix=indexprefix1, nop=nop,
                              consensustype=consensustype, newtags=newtags)

    for candId, tags in iteritems(consensus):
        # check remaining docs
        docids = find_docids(indexprefix=indexprefix1, candId=candId)

        # move candId
        result = copy_doc(indexprefix1+'cands', indexprefix2+'cands', candId,
                          deleteorig=True)

        if result:
            logger.info("Moved candId {0} from {1} to {2}"
                        .format(candId, indexprefix1, indexprefix2))

            # set tags field
            update_field(indexprefix2+'cands', 'tags',
                         consensus[candId]['tags'], Id=candId)

            # update png_url to new prefix and move plot
            png_url = get_doc(index=indexprefix1+'cands', Id=candId)['_source']['png_url']
            update_field(indexprefix2+'cands', 'png_url',
                         png_url.replace(indexprefix1, indexprefix2), Id=candId)
            candplot1 = ('claw@nmpost-master:/lustre/aoc/projects/fasttransients/realfast/plots/{0}/{1}.png'
                         .format(indexprefix1, candId))
            candplot2 = ('claw@nmpost-master:/lustre/aoc/projects/fasttransients/realfast/plots/{0}/{1}.png'
                         .format(indexprefix2, candId))
            success = rsync(candplot1, candplot2)
            if success:
                logger.info("Updated png_url field for {0} from {1} to {2}"
                            .format(candId, indexprefix1, indexprefix2))
            else:
                logger.warn("Could not update png_url field for {0} from {1} to {2}"
                            .format(candId, indexprefix1, indexprefix2))

            # if no candIds remain, then move remaining docs
            if len(docids[indexprefix1+'cands']) == 0:
                scanId = docids[indexprefix1+'scans']
                logger.info("{0} is last candidate in its scan. Moving remainind docs for scanId {1}."
                            .format(candId, scanId))
                for index1, idlist in docids:
                    index2 = index1.replace(indexprefix1, indexprefix2)
                    for Id0 in idlist:
                        result += copy_doc(index1, index2, Id0, deleteorig=True)

                logger.info("Moved {0} documents from {1} to {2}"
                            .format(result, indexprefix1, indexprefix2))

        else:
            logger.info("Failed move of CandId {0} from {1} to {2}"
                        .format(candId, indexprefix1, indexprefix2))


def find_docids(indexprefix, candId=None, scanId=None):
    """ Given a candId or scanId, find all associated docs.
    Finds relations based on scanId, which ties all docs together.
    Returns a dict with keys of the index name and values of the related ids.
    A full index set has:
        - cands indexed by candId (has scanId field)
        - scans indexed by scanId
        - preferences indexed by preferences name (in scans index)
        - mocks indexed by scanId (has scanId field)
        - noises indexed by noiseId (has scanId field)
    """

    docids = {}

    # option 1: give a candId to get scanId and then other docs
    if candId is not None and scanId is None:
        index = indexprefix + 'cands'
        doc_type = index.rstrip('s')

        doc = es.get(index=index, doc_type=doc_type, id=candId)
        scanId = doc['_source']['scanId']

    # option 2: use scanId given as argument or from above
    if scanId is not None:
        # use scanId to get ids with one-to-many mapping
        for ind in ['cands', 'mocks', 'noises']:
            index = indexprefix + ind
            ids = get_ids(index, scanId=scanId)
            docids[index] = ids

        # get prefsname from scans index
        index = indexprefix + 'scans'
        docids[index] = [scanId]
        prefsname = es.get(index=index, doc_type=index.rstrip('s'), id=scanId)['_source']['prefsname']
        index = indexprefix + 'preferences'
        docids[index] = [prefsname]

    return docids


def get_consensus(indexprefix='new', nop=3, consensustype='absolute',
                  res='consensus', newtags=None):
    """ Get candidtes with consensus over at least nop user tag fields.
    Argument consensustype: "absolute" (all agree), "majority" (most agree).
    Returns dicts with either consensus and noconsensus candidates.
    This includes original user tags plus new "tags" field with data state.
    newtags is a comma-delimited string that sets tags to apply to all.
    """

    assert consensustype in ["absolute", "majority"]
    assert res in ["consensus", "noconsensus"]
    if indexprefix == 'final':
        logger.warn("Looking at final indices, which should not be modified.")

    index = indexprefix+'cands'
    doc_type = index.rstrip('s')

    ids = []
    for n in range(nop, 10):  # do not expect more than 10 voters
        ids += get_ids(index=index, tagcount=nop)

    consensus = {}
    noconsensus = {}
    for Id in ids:
        doc = es.get(index=index, doc_type=doc_type, id=Id)
        tagsdict = dict(((k, v) for (k, v) in doc['_source'].items() if '_tags' in k))
        logger.debug("Id {0} has {1} tags: {2}".format(Id, len(tagsdict), tagsdict))
        tagslist = list(tagsdict.values())

        # add Id and tags to dict according to consensus opinion
        if consensustype == 'absolute':
            if all([tagslist[0] == val for val in tagslist]):
                tagsdict['tags'] = tagslist[0]
                if newtags is not None:
                    tagsdict['tags'] += ','+newtags
                consensus[Id] = tagsdict
            else:
                noconsensus[Id] = tagsdict
        elif consensustype == 'majority':
            # break out all tags (could be multiple per user)
            alltags = [tag for tags in tagslist for tag in tags.split(',')]

            # sort by whether tag is agreed upon by majority
            consensus_tags = []
            noconsensus_tags = []
            for tag in alltags:
                if alltags.count(tag) >= len(tagslist)//2+1:
                    consensus_tags.append(tag)
                else:
                    noconsensus_tags.append(tag)

            if newtags is not None:
                for newtag in newtags.split(','):
                    consensus_tags.append(newtag)

            if res == 'consensus':
                tagsdict['tags'] = ','.join(consensus_tags)
                consensus[Id] = tagsdict
            elif res == 'noconsensus':
                tagsdict['tags'] = ','.join(noconsensus_tags)
                noconsensus[Id] = tagsdict
        else:
            logger.exception("consensustype {0} not recognized"
                             .format(consensustype))

    if res == 'consensus':
        return consensus
    elif res == 'noconsensus':
        return noconsensus


def copy_doc(index1, index2, Id, deleteorig=False, force=False):
    """ Take doc in index1 with Id and move to index2
    Default is to copy, but option exists to "move" by deleting original.
    using force=True will override ban on operating from final indices.
    """

    if not force:
        assert 'final' not in index1

    doc_type1 = index1.rstrip('s')
    doc_type2 = index2.rstrip('s')

    doc = es.get(index=index1, doc_type=doc_type1, id=Id)
    res = es.index(index=index2, doc_type=doc_type2, id=Id,
                   body=doc['_source'])

    if res['_shards']['successful']:
        if deleteorig:
            res = es.delete(index=index1, doc_type=doc_type1, id=Id)
    else:
        logger.warn("Move of {0} from index {1} to {2} failed".format(Id,
                                                                      index1,
                                                                      index2))

    return res['_shards']['successful']


###
# Set up indices
###

def create_indices(indexprefix):
    """ Create standard set of indices,
    cands, scans, preferences, mocks, noises
    """

    body = {"settings": {
                "analysis": {
                    "analyzer": {
                        "default": {"tokenizer": "whitespace"}
                        }
                    }
                },
            }

    body_preferences = body.copy()
    body_preferences['mappings'] = {indexprefix+"preference": {
                                     "properties": {
                                       "flaglist": {"type":  "text"},
                                       "calcfeatures": {"type":  "text"}
                                       }
                                     }
                                    }

    indices = ['scans', 'cands', 'preferences', 'mocks', 'noises']
    for index in indices:
        fullindex = indexprefix+index
        if es.indices.exists(index=fullindex):
            confirm = input("Index {0} exists. Delete?".format(fullindex))
            if confirm:
                es.indices.delete(index=fullindex)
        if index != 'preferences':
            es.indices.create(index=fullindex, body=body)
        else:
            es.indices.create(index=fullindex, body=body_preferences)


def reset_indices(indexprefix, deleteindices=False):
    """ Remove entries from set of indices with a given indexprefix.
    indexprefix allows specification of set of indices ('test', 'aws').
    Use indexprefix='new' for production.
    deleteindices will delete indices, too.

    *BE SURE YOU KNOW WHAT YOU ARE DOING*
    """

    logger.warn("Erasing all docs from indices with prefix {0}"
                .format(indexprefix))

    for index in [indexprefix+'noises', indexprefix+'mocks',
                  indexprefix+'cands', indexprefix+'scans',
                  indexprefix+'preferences']:
        confirm = input("Confirm removal of {0} entries"
                        .format(index))
        res = 0
        if confirm:
            try:
                Ids = get_ids(index)
                for Id in Ids:
                    res += pushdata({}, index, Id, command='delete')
                logger.info("Removed {0} docs from index {1}".format(res, index))
                if deleteindices:
                    es.indices.delete(index)
                    logger.info("Removed {0} index".format(index))
            except TransportError:
                logger.info("Index {0} does not exist".format(index))


def rsync(original, new):
    """ Uses subprocess.call to rsync from 'filename' to 'new'
    If new is directory, copies original in.
    If new is new file, copies original to that name.
    """

    assert os.path.exists(original), 'Need original file!'
    res = subprocess.call(["rsync", "-a", original.rstrip('/'), new.rstrip('/')])

    return int(res == 0)
