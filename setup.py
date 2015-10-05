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
    scripts = ['scripts/sdm_chop-n-serve.pl', 'scripts/rqmanage.sh', 'scripts/queue_rtpipe.py', 'scripts/realfast.pl', 'scripts/listener.py'],   # add non-python scripts
    install_requires=[
        'Click',
        ],
    entry_points='''
        [console_scripts]
        queue_monitor=realfast.queue_monitor:monitor
        queue_empty=realfast.queue_monitor:empty
        queue_clean=realfast.queue_monitor:clean
        queue_status=realfast.queue_monitor:status
        queue_requeue=realfast.queue_monitor:requeue
        queue_failed=realfast.queue_monitor:failed
        queue_reset=realfast.queue_monitor:reset
        queue_movetoarchive=realfast.queue_monitor:movetoarchive
        rqinfo_monitor=realfast.rqinfo_monitor:monitor
        mcaf_monitor=realfast.mcaf_monitor:monitor
    ''',
)
