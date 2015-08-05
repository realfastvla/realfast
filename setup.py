from setuptools import setup, find_packages
import glob

setup(
    name = 'realfast',
    description = 'Python scripts for running real-time data analysis at the VLA',
    author = 'Casey Law + SBS',
    author_email = 'caseyjlaw@gmail.com',
    version = '0.0',
    include_package_data=True,
    packages = find_packages(),
    data_files = [ ('conf', glob.glob('conf/*.conf'))],
    dependency_links = ['http://github.com/caseyjlaw/rtpipe', 'http://github.com/caseyjlaw/sdmpy', 'http://github.com/caseyjlaw/sdmreader'],
    scripts = ['scripts/choose_SDM_scans.pl', 'scripts/rqmanage.sh', 'scripts/queue_rtpipe.py', 'scripts/realfast.pl'],   # add non-python scripts
    install_requires=[
        'Click',
        ],
    entry_points='''
        [console_scripts]
        queue_monitor=realfast.queue_monitor:monitor
        rqempty=realfast.queue_monitor:empty
        rqfailed=realfast.queue_monitor:failed
        rqinfo_monitor=realfast.rqinfo_monitor:monitor
        mcaf_monitor=realfast.mcaf_monitor:monitor
    ''',
)
