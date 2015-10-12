import click, os
from realfast import rtutils

@click.command()
@click.argument('filename')
@click.option('--mode', help="'read', 'search'", default='read')
@click.option("--paramfile", help="parameters for rtpipe using python-like syntax (custom parser for now)", default='')
@click.option("--fileroot", help="Root name for data products (used by calibrate for now)", default='')
@click.option("--sources", help="sources to search. comma-delimited source names (substring matched)", default='')
@click.option("--scans", help="scans to search. comma-delimited integers.", default='')
@click.option("--intent", help="Intent filter for getting scans", default='TARGET')
def rtpipe(filename, mode, paramfile, fileroot, sources, scans, intent):
    """ Function for command-line access to queue_rtpipe
    """

    qpriority = 'default'

    scans = rtutils.getscans(filename, scans=scans, sources=sources, intent=intent)
    filename = os.path.abspath(filename)
    if paramfile:
        paramfile = os.path.abspath(paramfile)

    if mode == 'read':
        rtutils.read(filename, paramfile, fileroot)

    elif mode == 'search':
        lastjob = rtutils.search(qpriority, filename, paramfile, fileroot, scans=scans)  # default TARGET intent

