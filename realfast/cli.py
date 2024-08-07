from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import click
import shutil
import subprocess
import os
import glob
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.captureWarnings(True)
logger = logging.getLogger(__name__)
logger.setLevel(20)

vys_cfile = '/home/cbe-master/realfast/soft/vysmaw_apps/vys.conf'
default_preffile = '/lustre/evla/test/realfast/realfast.yml'


@click.group('realfast')
def cli():
    pass


@cli.command()
@click.option('--mode', default='line')
@click.option('--line', default=None)
@click.option('--filepath', default='/home/cbe-master/realfast/soft/logs/')
@click.option('--globstr', default='rf*log')
@click.option('--age', default=3600, type=int)
def grep(mode, line, filepath, globstr, age):
    """ Find line in files in path
    Also has modes 'badclose' (and more?), where line is set
    for user.
    """

    import os.path
    import glob
    import time
    now = time.time()

    if mode == 'line':
        assert line is not None
    elif mode == 'badclose':
        line = '6: 0}'

    files0 = list(glob.glob(os.path.join(filepath, globstr)))
    files1 = list(filter(lambda x: now-os.path.getmtime(x) < age, files0))
    print('Running grep on {1} files ({0} match globstr)\n'.format(len(files1), len(files0)))
    files = ' '.join(files1)
    callstr = 'grep "{0}" {1}'.format(line, files)
    if len(files1):
#        print(callstr)
        subprocess.call(callstr, shell=True)
    else:
        print('No files found')

@cli.command()
@click.option('--channel', default='#alerts')
@click.option('--message')
def slack(channel, message):
    """ Update slack with something.                                                                                                                                                             
    """

    from realfast import util
    util.update_slack(channel, message)


@cli.command()
@click.option('--preffile', default='realfast.yml')
@click.option('--inprefs', default={})
def config_catcher(preffile, inprefs):
    """ Runs async process to catch multicast messages to form scan config objects
    Can be saved to pklfile and optionally attached to preferences from preffile.
    """

    from realfast import controllers

    logger.info("Running config catcher with preffile={0}"
                .format(preffile))
    config = controllers.config_controller(preffile=preffile, inprefs=inprefs)
    config.run()


@cli.command()
@click.option('--mode', default='deployment')
@click.option('--preffile', default=default_preffile)
@click.option('--threshold', type=float, default=None)
@click.option('--excludeants', default=None)
def run(mode, preffile, threshold, excludeants):
    """ Run realfast controller to catch scan configs and start rfpipe.
    mode can be "deployment" or "development", which defines scheduler IP.
    preffile can be realfast.yml or another yml config file.
    """

    from realfast import controllers

    if mode == 'deployment':
        host = '10.80.200.201:8786'
    elif mode == 'development':
        host = '10.80.200.201:8796'
    else:
        logger.warn("mode not recognized (deployment or development allowed)")
        return 1

    # overload some preferences
    inprefs = {}
    if threshold is not None:
        inprefs['sigma_image1'] = threshold
    if excludeants is not None:
        inprefs['excludeants'] = excludeants

    try:
        rfc = controllers.realfast_controller(host=host, preffile=preffile,
                                              inprefs=inprefs)
                                              
        rfc.initialize()
        rfc.run()
    except KeyboardInterrupt:
        logger.warn("Cleaning up before stopping processing.")
        rfc.cleanup_loop()
    except OSError:
        logger.warn("Could not start controller. Are scheduler and workers running?")


@cli.command()
@click.option('--sdmname', default=None)
@click.option('--candid', default=None)
@click.option('--indexprefix', default='new')
@click.option('--copybdf', is_flag=True)
def buildsdm(sdmname, candid, indexprefix, copybdf):
    """ Assemble sdm/bdf from cbe lustre.
    Can find it from sdmname or can look up by candid.
    """

    from realfast import util

    util.buildsdm(sdmname, candid, indexprefix=indexprefix, copybdf=copybdf)


@cli.command()
@click.option('--globstr', default='/home/mctest/evla/mcaf/workspace/realfast*')
def backup(globstr):
    """ Get all SDMs in sdm building directory and run buildsdm on them to save locally.
    """

    import glob
    default2 = '/home/mctest/evla/mcaf/workspace/realfast-archived'

    sdmnames = glob.glob(globstr)
    sdmnames += glob.glob(default2)
    for sdmname in sdmnames:
        sdmname = os.path.basename(sdmname)
        if not os.path.exists(sdmname):
            args = ["realfast", "buildsdm", "--sdmname", sdmname]
            logger.info("building sdm {0} locally".format(sdmname))
            subprocess.call(args)
        else:
            logger.info("sdm {0} already exists locally".format(sdmname))


@cli.command()
@click.argument('sdmname')
@click.option('--notebook', default='Search_and_refine.ipynb')
@click.option('--on_rfnode', type=bool, default=True)
@click.option('--preffile', default=None)
def refinement_notebook(sdmname, notebook, on_rfnode, preffile):
    """ Compile notebook
    """

    notebookpath = '/home/cbe-master/realfast/soft/realfast/realfast/notebooks'

    # report-mode just shows output of each cell
    args = ["papermill", "--report-mode", "-p", "sdmname", sdmname, os.path.join(notebookpath, notebook), sdmname+".ipynb"]
    subprocess.call(args)
    args = ["jupyter", "nbconvert", sdmname+".ipynb", "--to", "html", "--output", sdmname+".html"]
    subprocess.call(args)
    destination = 'claw@nmpost-master:/lustre/aoc/projects/fasttransients/realfast/plots/refinement'
    args = ["rsync", "-av", "--remove-source-files", "--include", "{0}.html".format(sdmname), "--exclude", "*", '.', destination]
    logger.info("Refinement notebook available at http://realfast.nrao.edu/plots/refinement/{0}.html".format(sdmname))
    status = subprocess.call(args)
    if not status:
        os.remove(sdmname + '.ipynb')

@cli.command()
@click.argument('candid')
@click.option('--indexprefix', default='new')
@click.option('--ddm', default=50)
@click.option('--mode', default='deployment')
@click.option('--npix_max', default=8196)
def refine_candid(candid, indexprefix, ddm, mode, npix_max):
    """ Compile notebook
    """

    from realfast import util
    
    util.refine_candid(candid, indexprefix=indexprefix, ddm=ddm, mode=mode, npix_max=npix_max)


@cli.command()
@click.argument('query')
@click.option('--indexprefix', default='new')
@click.option('--confirm', default=True, type=bool)
@click.option('--mode', default='deployment')
@click.option('--npix_max', default=8196)
def refine_all(query, indexprefix, confirm, mode, npix_max):
    """ Refines all candidates matching query
    """

    from realfast import elastic, util
    import distributed

    Ids = elastic.get_ids(indexprefix+'cands', query)

    yn = 'yes'
    if confirm:
        yn = input("Refine {0} candidates matching query {1}?".format(len(Ids), query))

    if mode == 'deployment':
        host = '10.80.200.201:8786'
    elif mode == 'development':
        host = '10.80.200.201:8796'
    else:
        logger.warn("mode not recognized (deployment or development allowed)")
        return

    cl = distributed.Client(host)
    if yn.lower() in ['y', 'yes']:
        for Id in Ids: 
            util.refine_candid(Id, indexprefix=indexprefix, mode=mode, cl=cl, npix_max=npix_max)
    cl.close()

    
@cli.command()
@click.option('--confirm', default=True, type=bool)
@click.option('--mode', default='archive')
def archive_local(confirm, mode):
    """ Move data from lustre workdir into local archive
    mode can be 'archive' or 'tests' and defines directory where products are moved.
    """
    
    import datetime
    import os
    import glob
    import shutil

    assert mode in ['tests', 'archive']

    if os.getcwd() == '/lustre/evla/test/realfast':
        now = datetime.datetime.now()
        dirname = '{0}/{1}{2}{3:02}'.format(mode, now.year-2000, now.strftime("%b").lower(), now.day)
        if not os.path.exists(dirname): 
            os.mkdir(dirname) 
            logger.info("Creating directory {0} for products".format(dirname))
        filelist = glob.glob("cands_*[html|pkl|png]")
        xmlfilelist = glob.glob("rfcand*xml")
        if len(xmlfilelist):
            logger.info("Found {0} xml files.".format(len(xmlfilelist)))
        filelist += xmlfilelist

        yn = 'yes'
        if confirm:
            yn = input("Move {0} files to {1}?".format(len(filelist), dirname))

        if yn.lower() in ['y', 'yes']:
            for fp in filelist:
                try:
                    shutil.move(fp, dirname)
                except shutil.Error:
                    logger.info("File {0} already exists. Skipping".format(fp))
    else:
        logger.warn("Need to be on CBE in lustre workdir")


@click.group('realfast_portal')
def cli2():
    pass


@cli2.command()
@click.argument('index')
def get_ids(index):
    """ Get ids in given index
    """

    from realfast import elastic

    logger.info("Getting Ids in index {0}".format(index))
    print(elastic.get_ids(index))


@cli2.command()
@click.argument('index')
@click.argument('_id')
def get_doc(index, _id):
    """ Get doc with _id in given index
    """

    from realfast import elastic

    logger.info("Getting Id {0} in index {1}".format(_id, index))
    print(elastic.get_doc(index, _id))


@cli2.command()
@click.option('--prefix1', default='new')
@click.option('--prefix2', default='final')
@click.option('--datasetid', default=None)
@click.option('--scanid', default=None)
@click.option('--force', is_flag=True)
def move_dataset(prefix1, prefix2, datasetid, scanid, force):
    """ Move datasetId or scanId from prefix1 to prefix2
    """

    from realfast import elastic

    elastic.move_dataset(prefix1, prefix2, datasetId=datasetid, scanId=scanid, force=force)


@cli2.command()
@click.option('--consensusstr', type=str, default=None)
@click.option('--consensusfile', type=click.File('rb'), default=None)
@click.option('--nop', default=3, help="Number of tags required to get consensus")
@click.option('--decidable', default=True, help="Only move those with decidable consensus")
@click.option('--prefix1', default='new', help="Moving from this index prefix")
@click.option('--prefix2', default='final', help="Moving to this index prefix")
def move_consensus(consensusstr, consensusfile, nop, decidable, prefix1, prefix2):
    """ Use consensus to move candidates from 1 to 2 with a given consensus.
    Can be executed remotely (from rfnode on aoc) or to find consensus locally.
    """

    from realfast import elastic
    import json

    # three options to get consensus
    if consensusstr is None and consensusfile is None:
        consensus = elastic.get_consensus(consensustype='majority', nop=nop)
    elif consensusfile is not None:
        with consensusfile:
            consensusstr = consensusfile.read()
        consensus = json.loads(consensusstr)
    elif consensusstr is not None:
        consensus = json.loads(consensusstr)

    if decidable:
        delete, archive = elastic.consensus_decision(consensus)
        consensus = {}
        for candId in delete + archive:
            consensus[candId] = {'tags': []}

    elastic.move_consensus(consensus=consensus, indexprefix1=prefix1, indexprefix2=prefix2, force=True)


@cli2.command()
@click.option('--nop', default=3)
@click.option('--confirm', type=bool, default=True)
def run_remove_bad(nop, confirm):
    """ Finds candidates with a consensus 'bad' tag and removes bdfs (at VLA)
    and/or moves them to 'final' index (at AOC).
    """

    from realfast import elastic
    
    con = elastic.get_consensus(consensustype='majority', nop=nop, match='bad')
        
    yn = 'yes'
    if confirm:
        if os.getcwd() == '/lustre/evla/test/realfast':  # at VLA on CBE/lustre
            yn = input(f"Remove BDFs for {len(con)} candidates?")
        else:
            yn = input(f"Move {len(con)} candids to final?")

    if yn.lower() in ['y', 'yes']:
        if os.getcwd() == '/lustre/evla/test/realfast':  # at VLA on CBE/lustre
            print('Deleting BDFs...')
            elastic.remove_bdfs('new', con.keys())
        else:
            print('Moving candids...')
            elastic.move_consensus(indexprefix1='new', indexprefix2='final', force=True, consensus=con)


@cli2.command()
@click.option('--nop', default=3)
@click.option('--confirm', type=bool, default=True)
def run_consensus_rfnode(nop, confirm):
    """ Go to rfnode021 and run the consensus to find "delete/rfi" or "archive/astrophysical" groups.
    Then delete the bad and build SDMs for the good.
    nop is number of tags required to get consensus.
    confirm is boolean to check whether to confirm before deleting/building SDMs.
    """

    from realfast import elastic, util

    assert os.path.exists('/lustre/evla/test/realfast'), 'Must be run at VLA site'

    con = elastic.get_consensus(consensustype='majority', nop=nop)

    delete, archive = elastic.consensus_decision(con)

    yn = 'yes'
    if confirm:
        yn = input(f"Found {len(delete)} BDFs for deletion and {len(archive)} to build SDMs.\n"
        f"Start of delete list: {delete[:10]}\n"
        f"Start of archive list: {archive[:10]}.\n"
        "Delete and build SDMs for these candIds?")

    if yn.lower() in ['y', 'yes']:
        for candId in archive:
            try:
                util.buildsdm(sdmname=None, candid=candId, indexprefix='new', copybdf=True)
            except AssertionError:
                logger.info(f'candId {candId} SDM not made.')

        elastic.remove_bdfs('new', delete)


@cli2.command()
@click.argument('candid')
@click.option('--prefix1', default='new')
@click.option('--prefix2', default='final')
@click.option('--force', is_flag=True)
def move_candid(candid, prefix1, prefix2, force):
    """ Move candidates from 1 to 2.
    """

    from realfast import elastic
    consensus = {candid: {'tags':[]}}
    elastic.move_consensus(consensus=consensus, indexprefix1=prefix1, indexprefix2=prefix2, force=force)


@cli2.command()
@click.option('--prefix', default='new')
@click.option('--datasetid', default=None)
@click.option('--scanid', default=None)
@click.option('--force', is_flag=True)
def remove_dataset(prefix, datasetid, scanid, force):
    """ Remove all data associated with given datasetid
    If datasetid is "test", it removes data with "test", "otf_holo", "TFST0001", "TSKY0001", and "mimic-vlass" in the datasetId.
    """

    from realfast import elastic

    if datasetid.lower() == "test":
        testids = set(['.'.join(Id.split('.')[:-2])  for Id in elastic.get_ids(f'{prefix}scans') if 'test' in Id or 'otf_holo' in Id or 'TFST0001' in Id or 'TSKY0001' in Id or 'mimic-vlass' in Id])
        for datasetid in testids:
            elastic.remove_dataset(prefix, datasetId=datasetid, scanId=scanid, force=force)
    else:
        elastic.remove_dataset(prefix, datasetId=datasetid, scanId=scanid, force=force)

@cli2.command()
@click.argument('datasetid', default=None)
def move_test(datasetid):
    """ Force move all data associated with test datasetId from "new" to "test".
    if datasetid is "all", then all with "test" in name are moved.
    """

    from realfast import elastic

    if datasetid.lower() != "all":
        elastic.move_dataset("new", "test", datasetId=datasetid, force=True)
    else:
        testids = set(['.'.join(Id.split('.')[:-2])  for Id in elastic.get_ids('newscans') if 'test' in Id or 'otf_holo' in Id or 'TFST0001' in Id or 'TSKY0001' in Id or 'mimic-vlass' in Id])
        print(f"Moving all datasetIds {testids}")
        for datasetid in testids:
            elastic.move_dataset("new", "test", datasetId=datasetid, force=True)


@cli2.command()
@click.argument('prefix')
def audit_indexprefix(prefix):
    """ Audit all indices with given prefix
    """

    from realfast import elastic

    elastic.audit_indexprefix(prefix)


@cli2.command()
@click.argument('prefix')
def reset_indices(prefix):
    """ Reset all indices with given prefix
    """

    from realfast import elastic

    elastic.reset_indices(prefix)
