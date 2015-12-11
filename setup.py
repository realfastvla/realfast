from setuptools import setup, find_packages
import glob

setup(
    name = 'realfast',
    description = 'Python scripts for running real-time data analysis at the VLA',
    author = 'Casey Law + SBS',
    author_email = 'caseyjlaw@gmail.com',
    version = '1.21',
    url = 'http://github.com/caseyjlaw/realfast',
    include_package_data=True,
    packages = find_packages(),
    data_files = [ ('conf', glob.glob('conf/*.conf'))],
    dependency_links = ['http://github.com/caseyjlaw/rtpipe', 'http://github.com/caseyjlaw/sdmpy', 'http://github.com/caseyjlaw/sdmreader'],
    scripts = ['scripts/sdm_chop-n-serve.pl', 'scripts/rqmanage.sh', 'scripts/queue_rtpipe.py', 'scripts/realfast.pl', 'scripts/listener.py'],   # add non-python scripts
    install_requires=[
        'Click',
        ],
    entry_points='''
        [console_scripts]
        queue_monitor=realfast.queue_monitor:monitor
        queue_empty=realfast.cli:empty
        queue_clean=realfast.cli:clean
        queue_status=realfast.cli:status
        queue_requeue=realfast.cli:requeue
        queue_failed=realfast.cli:failed
        queue_reset=realfast.cli:reset
        queue_movetoarchive=realfast.cli:manualarchive
        rqinfo_monitor=realfast.rqinfo_monitor:monitor
        mcaf_monitor=realfast.mcaf_monitor:monitor
        queue_rtpipe=realfast.cli:rtpipe
        queue_slowms=realfast.cli:slowms
    ''', 
)
