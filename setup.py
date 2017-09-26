from setuptools import setup, find_packages
import glob

setup(
    name='realfast',
    description='Real-time data analysis at the VLA',
    author='Casey Law + SBS',
    author_email='caseyjlaw@gmail.com',
    version='2.0',
    url='http://realfast.io',
    include_package_data=True,
    packages=find_packages(),
    data_files=[('conf', glob.glob('conf/*'))],
#    scripts=['scripts/sdm_chop-n-serve.pl'],   # add non-python scripts
    install_requires=['rfpipe', 'evla_mcast', 'sdmpy', 'click'],
    entry_points='''
        [console_scripts]
        rtpipe=realfast.cli:cli
'''
)
