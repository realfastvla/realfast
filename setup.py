from setuptools import setup, find_packages
setup(
    name = 'realtime',
    description = 'Python scripts for running real-time data analysis at the VLA',
    author = 'Casey Law',
    author_email = 'caseyjlaw@gmail.com',
    version = '0.0',
    packages = find_packages(),        # get all python scripts in realtime
    dependency_links = ['http://github.com/caseyjlaw/rtpipe', 'http://github.com/caseyjlaw/sdmpy', 'http://github.com/caseyjlaw/sdmreader'],
    scripts = ['choose_SDM_scans.pl', 'rqmanage.sh'],   # add non-python scripts
    py_modules=['mcaf_monitor', 'queue_monitor', 'queue_rtpipe', 'rqinfo_monitor', 'rqsettings_aoc', 'rqsettings_cbe'],    # set up for click
    install_requires=[
        'Click',
        ],
    entry_points='''
        [console_scripts]
        tailmon=queue_monitor:monitor
        rqmon=rqinfo_monitor:run
        queue_rtpipe=queue_rtpipe
        mcafmon=mcaf_monitor
    ''',
)
