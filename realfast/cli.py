from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import click
import shutil
import os
import glob
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.captureWarnings(True)
logger = logging.getLogger(__name__)

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
    import subprocess
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
def run(mode, preffile):
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

    try:
        rfc = controllers.realfast_controller(host=host, preffile=preffile)
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
@click.option('--copybdf', type=bool, default=True)
def buildsdm(sdmname, candid, indexprefix, copybdf):
    """ Assemble sdm/bdf from cbe lustre.
    Can find it from sdmname or can look up by candid.
    """

    if sdmname is None:
        from realfast import elastic
        if candid is None:
            logger.exception("Need to provide canid or sdmname")
        doc = elastic.get_doc(indexprefix + 'cands', candid)
        assert 'sdmname' in doc['_source'], 'No sdmname associated with that candid'
        sdmname = doc['_source']['sdmname'].split('/')[-1]
        logger.info("Got sdmname {0} from {1}cands index".format(sdmname, indexprefix))

    sdmloc = '/home/mctest/evla/mcaf/workspace/'
    sdmname_full = os.path.join(sdmloc, sdmname)

    shutil.copytree(sdmname_full, os.path.join('.', sdmname))

    bdfdestination = os.path.join('.', sdmname, 'ASDMBinary')
    os.mkdir(bdfdestination)

    bdft = sdmname.split('_')[-1]
    # remove suffix for sdms created multiple times
    if bdft[-2] is '.':
        bdft = bdft[:-2]
    bdfdir = '/lustre/evla/wcbe/data/realfast/'
    bdf0 = glob.glob('{0}/*{1}'.format(bdfdir, bdft))
    if len(bdf0) == 1:
        bdf0 = bdf0[0].split('/')[-1]
        if copybdf:
            shutil.copy(os.path.join(bdfdir, bdf0), os.path.join(bdfdestination, bdf0))
        else:
            os.symlink(os.path.join(bdfdir, bdf0), os.path.join(bdfdestination, bdf0))
    elif len(bdf0) == 0:
        logger.warn("No bdf found for {0}".format(sdmname))
    else:
        logger.warn("Could not find unique bdf for {0} in {1}. No bdf copied.".format(sdmname, bdf0))


@cli.command()
@click.option('--globstr', default='/home/mctest/evla/mcaf/workspace/realfast*')
def backup(globstr):
    """ Get all SDMs in sdm building directory and run buildsdm on them to save locally.
    """

    import subprocess, os, glob

    sdmnames = glob.glob(globstr)
    for sdmname in sdmnames:
        sdmname = os.path.basename(sdmname)
        if not os.path.exists(sdmname):
            args = ["realfast", "buildsdm", "--sdmname", sdmname, "--copybdf", "False"]
            logger.info("building sdm {0} locally".format(sdmname))
            subprocess.call(args)
        else:
            logger.info("sdm {0} already exists locally".format(sdmname))


@cli.command()
@click.argument('sdmname')
@click.option('--notebook', default='Search_and_refine.ipynb')
@click.option('--on_rfnode', type=bool, default=True)
@click.option('--preffile', default=None)
def refine(sdmname, notebook, on_rfnode, preffile):
    """ Compile notebook
    """
    import subprocess, os
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
@click.argument('datasetid')
def move_dataset(prefix1, prefix2, datasetid):
    """ Move datasetId from prefix1 to prefix2
    """

    from realfast import elastic

    elastic.move_dataset(prefix1, prefix2, datasetid)


@cli2.command()
@click.option('--prefix1', default='new')
@click.option('--prefix2', default='final')
def move_consensus(prefix1, prefix2):
    """ Use consensus to move candidates from 1 to 2
    """

    from realfast import elastic


@cli2.command()
@click.option('--prefix', default='new')
@click.argument('datasetid')
def remove_dataset(prefix, datasetid):
    """ Remove all data associated with given datasetid
    """

    from realfast import elastic

    elastic.move_dataset(prefix, None, datasetid)


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
