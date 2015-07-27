from setuptools import setup, find_packages
setup(
    name = 'realfast',
    description = 'Python scripts for running real-time data analysis at the VLA',
    author = 'Casey Law',
    author_email = 'caseyjlaw@gmail.com',
    version = '0.0',
    packages = find_packages(),
    dependency_links = ['http://github.com/caseyjlaw/rtpipe', 'http://github.com/caseyjlaw/sdmpy', 'http://github.com/caseyjlaw/sdmreader'],
    scripts = ['choose_SDM_scans.pl', 'rqmanage.sh', 'queue_rtpipe.py'],   # add non-python scripts
    py_modules=['mcaf_monitor', 'queue_monitor', 'rqinfo_monitor'],    # set up for click
    install_requires=[
        'Click',
        ],
    entry_points='''
        [console_scripts]
        queue_monitor=queue_monitor:monitor
        rqinfo_monitor=rqinfo_monitor:monitor
        mcaf_monitor=realfast.mcaf_monitor
    ''',
)
