from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import os.path
import subprocess
import shutil
from time import sleep
from elasticsearch import Elasticsearch, RequestError, TransportError, helpers, NotFoundError
from urllib3.connection import ConnectionError, NewConnectionError
import logging
logging.getLogger('elasticsearch').setLevel(30)
logger = logging.getLogger(__name__)
logger.setLevel(20)

# eventually should be updated to search.realfast.io/api with auth
es = Elasticsearch(['realfast.nrao.edu:9200'], timeout=30, max_retries=3, retry_on_timeout=True)


###
# Indexing stuff
###

def indexscan(config=None, inmeta=None, sdmfile=None, sdmscan=None,
              sdmsubscan=1, bdfdir=None, preferences=None, datasource=None,
              indexprefix='new'):
    """ Index properties of scan.
    Uses data source (config, sdm, etc.) to define metadata object.

    """

    from rfpipe.metadata import make_metadata
    from numpy import degrees
    from realfast import heuristics

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
    scandict['ra'] = float(ra)
    scandict['dec'] = float(dec)
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


def indexscanstatus(scanId, indexprefix='new', **kwargs):
    """ Update status fields for scanId
        Can set field of 'nsegment', 'pending', 'finished', 'errors'.
    """

    index = indexprefix+'scans'

    allowed = ['nsegment', 'pending', 'finished', 'errors']
    fieldlist = [field for (field, value) in iteritems(kwargs)
                 if field in allowed]
    valuelist = [int(value) for (field, value) in iteritems(kwargs)
                 if field in allowed]
    try:
        res = update_fields(index, fieldlist, valuelist, scanId)
        if res:
            logger.info("Updated processing status of {0} fields for {1}"
                        .format(len(fieldlist), scanId))
        else:
            logger.warn("Update of status for scan {0} failed".format(scanId))
    except TransportError:
        logger.warn("Could not update fields due to version conflict. Skipping this update.")


def indexprefs(preferences, indexprefix='new'):
    """ Index preferences with id equal to hash of contents.
    indexprefix allows specification of set of indices ('test', 'new').
    Use indexprefix='new' for production.
    """

    index = indexprefix+'preferences'
    res = pushdata(preferences.ordered, index=index,
                   Id=preferences.name, command='index')
    if res == 1:
        logger.info('Indexed preference {0} to {1}'
                    .format(preferences.name, index))
    else:
        logger.debug('Preferences not indexed for {0}'.format(preferences.name))


def indexcands(candcollection, scanId, tags=None, url_prefix=None,
               indexprefix='new'):
    """ Takes candidate collection and pushes to index
    Connects to preferences via hashed name
    scanId is added to associate cand to a give scan.
    Assumes scanId is defined as:
    datasetId dot scanNo dot subscanNo.
    tags is a comma-delimited string used to fill tag field in index.
    indexprefix allows specification of set of indices ('test', 'new').
    Use indexprefix='new' for production.
    """

    from numpy import degrees, cos

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

        # get reference ra, dec
        segment = canddict['segment']
        if candcollection.state.otfcorrections is not None:
            ints, ra_ctr, dec_ctr = candcollection.state.otfcorrections[segment][0]  # TODO: radians or degrees returned?
        else:
            ra_ctr, dec_ctr = candcollection.metadata.radec  # radians

        # fill optional fields
        canddict['scanId'] = scanId
        datasetId, scan, subscan = scanId.rsplit('.', 2)
        canddict['datasetId'] = datasetId
        canddict['scan'] = int(scan)
        canddict['subscan'] = int(subscan)
        canddict['source'] = candcollection.metadata.source
        canddict['tags'] = tagstr
        canddict['tagcount'] = 0
        canddict['candmjd'] = float(candmjd[i])
        canddict['canddm'] = float(canddm[i])
        canddict['canddt'] = float(canddt[i])
        canddict['cluster'] = int(cluster[i])
        canddict['clustersize'] = int(clustersize[i])
        canddict['snrtot'] = float(snrtot[i])
        canddict['ra'] = degrees(ra_ctr + canddict['l1']/cos(dec_ctr))
        canddict['dec'] = degrees(dec_ctr + canddict['m1'])
        canddict['png_url'] = ''
        if prefs.name:
            canddict['prefsname'] = prefs.name

        # create id
        uniqueid = candid(datadict=canddict)
        canddict['candId'] = uniqueid
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


def indexmock(scanId, mocks=None, acc=None, indexprefix='new'):
    """ Takes simulated_transient as used in state and pushes to index.
    Assumes 1 mock in list for now.
    indexprefix allows specification of set of indices ('test', 'new').
    Use indexprefix='new' for production.
    Option to submit mocks as tuple or part of analyze_cc future.
    """

    from distributed import Future

    # for realtime use
    if mocks is None and acc is not None:
        if isinstance(acc, Future):
            ncands, mocks = acc.result()
        else:
            ncands, mocks = acc

    if mocks is not None:
        if len(mocks[0]) != 7:
            logger.warn("mocks not in expected format ({0})".format(mocks))

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
    if mocks is None or res == 0:
        logger.info('No mocks indexed for {0}'.format(scanId))


def indexnoises(scanId, noises=None, noisefile=None, indexprefix='new'):
    """ Takes noises as list or from noisefile and pushes to index.
    scanId is added to associate cand to a give scan.
    indexprefix allows specification of set of indices ('test', 'new').
    """

    index = indexprefix+'noises'
    doc_type = index.rstrip('s')

    if noisefile is not None and noises is None:
        from rfpipe.candidates import iter_noise
        if os.path.exists(noisefile):
            logger.info("Reading noises from {0}".format(noisefile))
            noises = list(iter_noise(noisefile))

    assert isinstance(noises, list)

    count = 0
    for noise in noises:
        startmjd, deltamjd, segment, integration, noiseperbl, zerofrac, imstd = noise
        Id = '{0}.{1}.{2}'.format(scanId, segment, integration)
        if not es.exists(index=index, doc_type=doc_type, id=Id):
            noisedict = {}
            noisedict['scanId'] = str(scanId)
            noisedict['startmjd'] = float(startmjd)
            noisedict['deltamjd'] = float(deltamjd)
            noisedict['segment'] = int(segment)
            noisedict['integration'] = int(integration)
            noisedict['noiseperbl'] = float(noiseperbl)
            noisedict['zerofrac'] = float(zerofrac)
            noisedict['imstd'] = float(imstd)

            count += pushdata(noisedict, Id=Id, index=index,
                              command='index')
        else:
            logger.warn("noise index {0} already exists".format(Id))

    if count:
        logger.info('Indexed {0} noises for {1} to {2}'
                    .format(count, scanId, index))

    if not count:
        logger.info('No noises indexed for {0}'.format(scanId))


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


def candid(datadict=None, cc=None):
    """ Returns id string for given data dict
    Assumes scanId is defined as:
    datasetId dot scanNum dot subscanNum
    """

    if datadict is not None and cc is None:
        scanId = datadict['scanId']
        segment = datadict['segment']
        integration = datadict['integration']
        dmind = datadict['dmind']
        dtind = datadict['dtind']
        return ('{0}_seg{1}-i{2}-dm{3}-dt{4}'
                .format(scanId, segment, integration, dmind, dtind))
    elif cc is not None and datadict is None:
        scanId = cc.metadata.scanId
        return ['{0}_seg{1}-i{2}-dm{3}-dt{4}'
                .format(scanId, segment, integration, dmind, dtind)
                for segment, integration, dmind, dtind, beamnum in cc.locs]


def update_field(index, field, value, Id=None, **kwargs):
    """ Replace an index's field with a value.
    Option to work on single Id or query the index with kwargs.
    Use with caution.
    """

    doc_type = index.rstrip('s')

    if Id is None:
        query = {"script": {"inline": "ctx._source.{0}='{1}'".format(field, value),
                            "lang": "painless"}}
        query['retry_on_conflict'] = 5
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


def update_fields(index, fieldlist, valuelist, Id):
    """ Updates multiple fields together for single Id.
    Safer than update_field, which can produce doc conflicts if run rapidly.
    """

    doc_type = index.rstrip('s')
    doc = es.get(index, doc_type, Id) 
    doc['_source'].update(dict(zip(fieldlist, valuelist))) 
    res = es.index(index, doc_type, body=doc['_source'], id=Id) 
    return res['_shards']['successful']


def remove_tags(prefix, **kwargs):
    """ Removes tags applied in portal
    Can use keyword args to select subset of all candidates in cands index
    """

    Ids = get_ids(prefix+"cands", **kwargs)
    logger.info("Removing tags from {0} candidates in {1}".format(len(Ids), prefix+"cands"))

    for Id in Ids:
        doc = get_doc(prefix+"cands", Id)
        tagnames = [key for key in doc['_source'].keys() if '_tags' in key]
        if len(tagnames):
            print("Removing tags {0} for Id {1}".format(tagnames, Id))
            for tagname in tagnames:
                resp = es.update(prefix+"cands", prefix+"cand", Id, {"script": 'ctx._source.remove("' + tagname + '")'})
            resp = es.update(prefix+"cands", prefix+"cand", Id, {"script": 'ctx._source.tagcount = 0'})


def remove_ids(index, Ids=None, check=True, **kwargs):
    """ Gets Ids from an index
    doc_type derived from index name (one per index)
    Can optionally pass key-value pairs of field-string to search.
    Must match exactly (e.g., "scanId"="test.1.1")
    """

    if Ids is None:
        # delete all Ids in index
        if not len(kwargs):
            logger.warn("No Ids or query kwargs. Clearing all Ids in {0}"
                        .format(index))
        Ids = get_ids(index, **kwargs)

    if check:
        confirm = input("Press any key to confirm removal of {0} ids from {1}."
                        .format(len(Ids), index))
    else:
        confirm = 'yes'

    res = 0
    if confirm.lower() in ['y', 'yes']:
        for Id in Ids:
            res += pushdata({}, index, Id, command='delete')

        logger.info("Removed {0} docs from index {1}".format(res, index))

    return res


def get_ids(index, *args, **kwargs):
    """ Gets Ids from an index
    doc_type derived from index name (one per index)
    Can optionally pass arg for string query.
    Can optionall pass query_string=<search field query>.
    Can optionally pass key-value pairs of field-string to search.
    Must match exactly (e.g., "scanId"="test.1.1")
    """

    # only one doc_type per index and its name is derived from index
    doc_type = index.rstrip('s')
    if 'field' in kwargs:
        field = kwargs.pop('field')
    else:
        field = 'false'

    if len(args):
        wildcard = '*' + '* *'.join(args) + '*'
        logger.debug("Using arg as wildcard")
        query = {"query":{"query_string": {"query": wildcard}}}
    elif "query_string" in kwargs:
        query = {"query":{"query_string": {"query": kwargs["query_string"]}}}
        logger.info("Using query_string kwargs only")
    elif len(kwargs):
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
# Using index
###

def create_preference(index, Id):
    """ Get doc from index with Id and create realfast preference object
    """

    from rfpipe.preferences import Preferences

    doc = get_doc(index, Id)
    prefs = doc['_source']
    return Preferences(**prefs)


###
# Managing docs between indexprefixes
###

def move_dataset(indexprefix1, indexprefix2, datasetId=None, scanId=None, force=False):
    """ Given two index prefixes, move a datasetId or scanId and all associated docs over.
    This will delete the original documents in indexprefix1.
    If indexprefix2 is None, then datasetId is removed from indexprefix1.
    """

    iddict0 = {indexprefix1+'cands': [], indexprefix1+'scans': [],
               indexprefix1+'mocks': [], indexprefix1+'noises': [],
               indexprefix1+'preferences': []}

    if scanId is None and datasetId is not None:
        scanids = get_ids(indexprefix1 + 'scans', datasetId=datasetId)
    else:
        scanids = [scanId]

    for scanId in scanids:
        iddict = copy_all_docs(indexprefix1, indexprefix2, scanId=scanId, force=force)
        for k, v in iddict.items():
            for Id in v:
                if Id not in iddict0[k]:
                    iddict0[k].append(Id)

    count = sum([len(v) for k, v in iteritems(iddict0)])
    if datasetId is not None:
        confirm = input("Remove dataset {0} from {1} with {2} ids in all indices?"
                        .format(datasetId, indexprefix1, count))
    if scanId is not None:
        confirm = input("Remove scanId {0} from {1} with {2} ids in all indices? {3}"
                        .format(scanId, indexprefix1, count, iddict0))

    # first remove Ids
    if confirm.lower() in ['y', 'yes']:
        for k, v in iddict0.items():
            if k != indexprefix1 + 'preferences':
                remove_ids(k, v, check=False)

            # remove old cand pngs
            if k == indexprefix1 + 'cands':
                for Id in v:
                    candplot1 = ('/lustre/aoc/projects/fasttransients/realfast/plots/{0}/cands_{1}.png'
                                 .format(indexprefix1, Id))
                    if os.path.exists(candplot1):
                        os.remove(candplot1)

            # remove old summary htmls
            if k == indexprefix1 + 'scans':
                logger.info("{0} {1}".format(k, v))
                for Id in v:
                    summary1 = ('/lustre/aoc/projects/fasttransients/realfast/plots/{0}/cands_{1}.html'
                                .format(indexprefix1, Id))
                    if os.path.exists(summary1):
                        os.remove(summary1)

        # test whether other scans are using prefsname
        prefsnames = iddict0[indexprefix1 + 'preferences']
        for prefsname in prefsnames:
            if not len(get_ids(indexprefix1 + 'scans', prefsname=prefsname)):
                remove_ids(indexprefix1 + 'preferences', [prefsname], check=False)
            else:
                logger.info("prefsname {0} is referred to in {1}. Not deleting"
                            .format(Id, k))

    # TODO: remove png and html files after last move


def copy_all_docs(indexprefix1, indexprefix2, candId=None, scanId=None, force=False):
    """ Move docs from 1 to 2 that are associated with a candId or scanId.
    scanId includes multiple candIds, which will all be moved.
    Specifying a candId will only move the candidate, scanId, and associated products (not all cands).
    Associated docs include scanId, preferences, mocks, etc.
    If scanId provided, all docs moved.
    If candId provided, only that one will be selected from all in scanId.
    If indexprefix2 is None, then the dict of all docs in indexprefix1 is returned.
    """

    if candId is not None:
        logger.info("Finding docs with candId {0}".format(candId))
    elif scanId is not None:
        logger.info("Finding docs with scanId {0}".format(scanId))

    iddict = find_docids(indexprefix1, candId=candId, scanId=scanId)
    if indexprefix2 is not None:
        assert os.path.exists('/lustre/aoc/projects/fasttransients/realfast/plots'), 'Only works on AOC lustre'
        for k, v in iddict.items():
            for Id in v:
                if (candId is not None) and (candId != Id) and (k == indexprefix1 + 'cands'):
                    pass
                try:
                    result = copy_doc(k, k.replace(indexprefix1, indexprefix2), Id, force=force)
                except NotFoundError:
                    logger.warn("Id {0} not found in {1}".format(Id, k))
                        
                # update png_url to new prefix and move plot
                if (k == indexprefix1+'cands') and result:
                    png_url = get_doc(index=indexprefix1+'cands', Id=Id)['_source']['png_url']
                    update_field(indexprefix2+'cands', 'png_url',
                                 png_url.replace(indexprefix1, indexprefix2),
                                 Id=Id)
                    candplot1 = ('/lustre/aoc/projects/fasttransients/realfast/plots/{0}/cands_{1}.png'
                                 .format(indexprefix1, Id))
                    candplot2 = ('/lustre/aoc/projects/fasttransients/realfast/plots/{0}/cands_{1}.png'
                                 .format(indexprefix2, Id))
                    if os.path.exists(candplot1):
                        shutil.move(candplot1, candplot2)
                        logger.info("Updated png_url field and moved plot for {0} from {1} to {2}"
                                    .format(Id, indexprefix1,
                                            indexprefix2))
#                        if os.path.exists(candplot1):
#                            os.remove(candplot1)
#                        else:
#                            logger.warn("Problem updating or moving png_url {0} from {1} to {2}"
#                                        .format(Id, indexprefix1,
#                                                indexprefix2))
                    else:
                        logger.warn("Could not find file {0}".format(candplot1))

                elif not result:
                    logger.info("Did not copy {0} from {1} to {2}"
                                .format(Id, indexprefix1, indexprefix2))

            # copy summary html file
            if k == indexprefix1+'scans':
                summary1 = ('/lustre/aoc/projects/fasttransients/realfast/plots/{0}/cands_{1}.html'
                            .format(indexprefix1, v[0]))
                summary2 = ('/lustre/aoc/projects/fasttransients/realfast/plots/{0}/cands_{1}.html'
                            .format(indexprefix2, v[0]))
                if os.path.exists(summary1):
                    success = shutil.copy(summary1, summary2)

    return iddict


def candid_bdf(indexprefix, candId):
    """ Given candId in indexprefix, list the bdfname, if it exists.
    """

    doc = get_doc(indexprefix+'cands', Id=candId) 
    if 'sdmname' in doc['_source']: 
        sdmname = doc['_source']['sdmname'] 
        logger.info("CandId {0} has sdmname {1}".format(candId, sdmname))
        bdfint = sdmname.split('_')[-1] 
        bdfname = '/lustre/evla/wcbe/data/realfast/uid____evla_realfastbdf_' + bdfint 
        if os.path.exists(bdfname):
            return bdfname
        else:
            logger.warn("No bdf found for {0}".format(sdmname))
            return None
    else:
        logger.warn("No SDM found for {0}".format(candId))
        return None


def remove_dataset(indexprefix, datasetId=None, scanId=None, force=False):
    """ Use dataset or scanId to remove bdfs, indexed data, and plots/html.
    On the CBE, this will remove bdfs, while at the AOC, it manages the rest.
    """

    if os.path.exists('/lustre/aoc/projects/fasttransients/realfast/plots'):
        logger.info("On the AOC, removing scanId from index and plots/html")
        move_dataset(indexprefix, None, datasetId=datasetId, scanId=scanId, force=force)
    else:
        logger.info("On the CBE, removing bdfs")
        if scanId is not None:
            Ids = get_ids(indexprefix + 'cands', scanId=scanId)
            Id = scanId
        elif datasetId is not None:
            Ids = get_ids(indexprefix + 'cands', datasetId=datasetId)
            Id = datasetId

        confirm = input("Remove bdfs associated with {0} candidates in {1}?".format(len(Ids), Id))
        if confirm.lower() in ['y', 'yes']:        
            remove_bdfs(indexprefix, Ids)

        
def remove_bdfs(indexprefix, candIds):
    """ Given candIds, look up bdf name in indexprefix and then remove bdf
    """

    for Id in candIds:
        bdfname = candid_bdf(indexprefix, Id) 
        if bdfname is not None: 
            os.remove(bdfname)
            logger.info('Removed {0}'.format(bdfname))

            
def find_docids(indexprefix, candId=None, scanId=None):
    """ Find docs associated with a candId or scanId.
    Finds relations based on scanId, which ties all docs together.
    Returns a dict with keys of the index name and values of the related ids.
    scanId includes multiple candIds, which will all be moved.
    Specifying a candId will only move the candidate, scanId, and associated products (not all cands).
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
        scanId = candId.split("_seg")[0]

    # option 2: use scanId given as argument or from above
    if scanId is not None:
        # use scanId to get ids with one-to-many mapping
        for ind in ['cands', 'mocks', 'noises']:
            index = indexprefix + ind
            try:
                ids = get_ids(index, scanId=scanId)
                # if searching by candId, then only define that one
                if ((candId is not None) and (ind == 'cands')):
                    docids[index] = [candId]
                else:
                    docids[index] = ids
            except NotFoundError:
                logger.warn("Id {0} not found in {1}".format(scanId, index))

        # get prefsname from scans index
        try:
            index = indexprefix + 'scans'
            docids[index] = [scanId]
            prefsname = es.get(index=index, doc_type=index.rstrip('s'), id=scanId)['_source']['prefsname']
            index = indexprefix + 'preferences'
            docids[index] = [prefsname]
        except NotFoundError:
            logger.warn("Id {0} not found in {1}".format(scanId, index))

    return docids


def audit_indexprefix(indexprefix):
    """ Confirm that all candids map to scanids, prefnames, and pngs.
    Confirm that scanids mocks, noises.
    Also test that candids have plots and summaryplots.
    """

    import requests

    scanIds = get_ids(indexprefix+'scans')
    candIds = get_ids(indexprefix+'cands')
    mockIds = get_ids(indexprefix+'mocks')
    noiseIds = get_ids(indexprefix+'noises')

    failed = 0
    for candId in candIds:
        doc = get_doc(indexprefix+'cands', candId)

        # 1) is candId tied to scanId?
        candIdscanId = doc['_source']['scanId']
        if candIdscanId not in scanIds:
            failed += 1
            logger.warn("candId {0} has scanId {1} that is not in {2}"
                        .format(candId, candIdscanId, indexprefix+'scans'))

        # 2) Is candId prefs indexed?
        prefsname = doc['_source']['prefsname']
        if prefsname not in get_ids(indexprefix+'preferences'):
            failed += 1
            logger.warn("candId {0} has prefsname {1} that is not in {2}"
                        .format(candId, prefsname, indexprefix+'preferences'))

        # 3) Is candId png_url in right place?
        png_url = doc['_source']['png_url']
        if requests.get(png_url).status_code != 200:
            failed += 1
            logger.warn("candId {0} png_url {1} is not accessible"
                        .format(candId, png_url))

        # 4) Does candId have summary plot?
        summary_url = ('http://realfast.nrao.edu/plots/{0}/cands_{1}.html'
                       .format(indexprefix, candIdscanId))
        if requests.get(summary_url).status_code != 200:
            failed += 1
            logger.warn("candId {0} summary plot {1} is not accessible"
                        .format(candId, summary_url))

    logger.info("{0} of {1} candIds have issues".format(failed, len(candIds)))

    failed = 0
    for scanId in scanIds:
        doc = get_doc(indexprefix+'scans', scanId)

        # 5) Is scanId prefs indexed?
        prefsname = doc['_source']['prefsname']
        if prefsname not in get_ids(indexprefix+'preferences'):
            failed += 1
            logger.warn("scanId {0} has prefsname {1} that is not in {2}"
                        .format(scanId, prefsname, indexprefix+'preferences'))

    logger.info("{0} of {1} scanIds have issues".format(failed, len(scanIds)))

    failed = 0
    for mockId in mockIds:
        doc = get_doc(indexprefix+'mocks', mockId)

        # 6) is mockId tied to scanId?
        mockIdscanId = doc['_source']['scanId']
        if mockIdscanId not in scanIds:
            failed += 1
            logger.warn("mockId {0} has scanId {1} that is not in {2}"
                        .format(mockId, mockIdscanId, indexprefix+'scans'))

    logger.info("{0} of {1} mockIds have issues".format(failed, len(mockIds)))

    failed = 0
    for noiseId in noiseIds:
        doc = get_doc(indexprefix+'noises', noiseId)

        # 7) is noiseId tied to scanId?
        noiseIdscanId = doc['_source']['scanId']
        if noiseIdscanId not in scanIds:
            failed += 1
            logger.warn("noiseId {0} has scanId {1} that is not in {2}"
                        .format(noiseId, noiseIdscanId, indexprefix+'scans'))

    logger.info("{0} of {1} noiseIds have issues".format(failed, len(noiseIds)))


def move_consensus(indexprefix1='new', indexprefix2='final', match='identical',
                   consensustype='majority', nop=3, newtags=None, consensus=None,
                   datasetId=None):
    """ Given candids, copies relevant docs from indexprefix1 to indexprefix2.
    newtags will append to the new "tags" field for all moved candidates.
    Default tags field will contain the user consensus tag.
    Can optionally define consensus elsewhere and pass it in.
    """

    if consensus is None:
        consensus = get_consensus(indexprefix=indexprefix1, nop=nop, match=match,
                                  consensustype=consensustype, newtags=newtags,
                                  datasetId=datasetId)

    logger.info("Moving {0} consensus candidates from {1} to {2}"
                .format(len(consensus), indexprefix1, indexprefix2))

    datasetIds = []
    for candId, tags in iteritems(consensus):
        scanId = candId.split('_seg')[0]
        datasetIds.append('.'.join(scanId.split('.')[:-2]))

        # check remaining docs
        iddict = copy_all_docs(indexprefix1, indexprefix2, candId=candId)

        # set tags field
        update_field(indexprefix2+'cands', 'tags',
                     consensus[candId]['tags'], Id=candId)

        res = pushdata({}, indexprefix1+'cands', candId, command='delete')

    # wait for transfer to complete
    sleep(3)

    # (re)move any datasets with no associated cands
    index = indexprefix1+'cands'
    datasetIds = set(datasetIds)
    logger.info("Checking whether datasetIds {0} remain in {1}".format(datasetIds, index))
    for datasetId in datasetIds:
        ncands = len(get_ids(index, datasetId=datasetId))
        if not ncands:
            logger.info("No cands from dataset {0} remain. Removing dataset from {1}"
                        .format(datasetId, indexprefix1))
            move_dataset(indexprefix1, indexprefix2, datasetId)
        else:
            logger.info("{0} candidates remain for dataset {1}".format(ncands, datasetId))


def get_consensus(indexprefix='new', nop=3, consensustype='majority',
                  res='consensus', match='identical', newtags=None,
                  datasetId=None):
    """ Get candidtes with consensus over at least nop user tag fields.
    Argument consensustype: "absolute" (all agree), "majority" (most agree).
    Returns dicts with either consensus and noconsensus candidates (can add "tags" too).
    match defines how tags are compared:
    - "identical" => string match,
    - "bad" => find rfi, instrumental, delete, unsure/noise in tags
    -- bad+absolute => all must be in bad list
    -- bad+majority => majority must be "delete".
    "notify" => notify found (only implemented for absolute).
    newtags is a comma-delimited string that sets tags to apply to all.
    Can optionally only consider candidates in datasetId.
    """

    assert consensustype in ["absolute", "majority"]
    assert res in ["consensus", "noconsensus"]
    if indexprefix == 'final':
        logger.warn("Looking at final indices, which should not be modified.")

    index = indexprefix+'cands'
    doc_type = index.rstrip('s')

    ids = []
    for ni in range(nop, 10):  # do not expect more than 10 voters
        idlist = get_ids(index=index, tagcount=ni)
        logger.info("Got {0} cands with tagcount={1}".format(len(idlist), ni))
        ids += idlist

    assert match in ['identical', 'bad', 'notify']

    badlist = ["rfi", "instrumental", "delete", "unsure/noise"]
    consensus = {}
    noconsensus = {}
    for Id in ids:
        # select only datasetId candidates, if desired
        if datasetId is not None and datasetId not in Id:
            continue

        tagsdict = gettags(indexprefix=indexprefix, Id=Id)
        logger.debug("Id {0} has {1} tags".format(Id, len(tagsdict)))
        tagslist = list(tagsdict.values())

        # add Id and tags to dict according to consensus opinion
        if consensustype == 'absolute':
            if match == 'identical':
                if all([tagslist[0] == val for val in tagslist]):
                    tagsdict['tags'] = tagslist[0]
                    if newtags is not None:
                        tagsdict['tags'] += ','+newtags
                    consensus[Id] = tagsdict
                else:
                    noconsensus[Id] = tagsdict
            elif match == 'bad':
                if all([tag in badlist for tags in tagslist for tag in tags.split(',')]):
                    tagsdict['tags'] = tagslist[0]
                    if newtags is not None:
                        tagsdict['tags'] += ','+newtags
                    consensus[Id] = tagsdict
                else:
                    noconsensus[Id] = tagsdict
            elif match == 'notify':
                if all(['notify' in tags for tags in tagslist]):
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
                if (tag in consensus_tags) or (tag in noconsensus_tags):
                    continue

                if (alltags.count(tag) >= len(tagslist)//2+1):
                    if match == 'identical':
                        consensus_tags.append(tag)
                    elif (match == 'bad') and (tag == 'delete'):
                        consensus_tags.append(tag)
                    else:
                        noconsensus_tags.append(tag)
                else:
                    noconsensus_tags.append(tag)

            if newtags is not None:
                for newtag in newtags.split(','):
                    consensus_tags.append(newtag)

            if res == 'consensus' and len(consensus_tags):
                tagsdict['tags'] = ','.join(consensus_tags)
                consensus[Id] = tagsdict
            elif res == 'noconsensus' and len(noconsensus_tags):
                tagsdict['tags'] = ','.join(noconsensus_tags)
                noconsensus[Id] = tagsdict
        else:
            logger.exception("consensustype {0} not recognized"
                             .format(consensustype))

    logger.info("Consensus found for {0} candidates in prefix {1}"
                .format(len(consensus), indexprefix))

    if res == 'consensus':
        logger.info("Returning consensus candidates")
        return consensus
    elif res == 'noconsensus':
        logger.info("Returning candidates without consensus")
        return noconsensus


def resolve_consensus(indexprefix='new', nop=3, consensustype='majority',
                      match='identical', datasetId=None):

    """ Step through noconsensus candidates and decide their fate.
    """

    nocon = get_consensus(indexprefix=indexprefix, nop=nop,
                          consensustype=consensustype, res='noconsensus',
                          match=match, datasetId=None)

    baseurl = 'http://realfast.nrao.edu/plots/' + indexprefix
    con = {}
    try:
        for k,v in nocon.items(): 
            logger.info("Candidate {0}:".format(k))
            logger.info("\tpng_url\t{0}/cands_{1}.png".format(baseurl, k))
            tags = v['tags'].split(',')
            logger.info("\tUser tags\t{0}".format([(vk, vv) for (vk, vv) in v.items() if '_tags' in vk]))
            selection = input("Set consensus tags (<int>,<int> or <cr> to skip): {0}".format(list(enumerate(tags))))
            if selection:
                selection = selection.replace(',', ' ').split()
                selection = reversed(sorted([int(sel) for sel in selection]))  # preserves order used in web app
                newtags = []
                for sel in selection:
                    newtags.append(tags[sel])
                v['tags'] = ','.join(newtags)
                con[k] = v
    except KeyboardInterrupt:
        logger.info("Escaping loop")
        return con

    return con


def gettags(indexprefix, Id):
    """ Get cand Id in for indexprefix
    return dict with all tags.
    """

    index = indexprefix+'cands'
    doc_type = index.rstrip('s')

    doc = es.get(index=index, doc_type=doc_type, id=Id)
    tagsdict = dict(((k, v) for (k, v) in doc['_source'].items() if '_tags' in k))
    return tagsdict


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
            if confirm.lower() in ['y', 'yes']:
                es.indices.delete(index=fullindex)
        if index != 'preferences':
            es.indices.create(index=fullindex, body=body)
        else:
            es.indices.create(index=fullindex, body=body_preferences)


def reset_indices(indexprefix, deleteindices=False):
    """ Remove entries from set of indices with a given indexprefix.
    indexprefix allows specification of set of indices ('test', 'new').
    Use indexprefix='new' for production.
    deleteindices will delete indices, too.

    *BE SURE YOU KNOW WHAT YOU ARE DOING*
    """

    logger.warn("Erasing all docs from indices with prefix {0}"
                .format(indexprefix))

    for index in [indexprefix+'noises', indexprefix+'mocks',
                  indexprefix+'cands', indexprefix+'scans',
                  indexprefix+'preferences']:
        res = remove_ids(index)
        if deleteindices:
            es.indices.delete(index)
            logger.info("Removed {0} index".format(index))


def rsync(original, new):
    """ Uses subprocess.call to rsync from 'filename' to 'new'
    If new is directory, copies original in.
    If new is new file, copies original to that name.
    """

    assert os.path.exists(original), 'Need original file!'
    res = subprocess.call(["rsync", "-a", original.rstrip('/'), new.rstrip('/')])

    return int(res == 0)
