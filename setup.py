from setuptools import setup, find_packages
import glob

setup(
    name='realfast',
    description='Real-time data analysis at the VLA',
    author='Casey Law and the realfast team',
    author_email='caseyjlaw@gmail.com',
    version='3.3.0',
    url='http://realfast.io',
    include_package_data=True,
    packages=find_packages(),
    package_data={'realfast': ['xsd/*.xsd']},
    data_files=[('conf', glob.glob('conf/*'))],
#    scripts=['scripts/sdm_chop-n-serve.pl'],   # add non-python scripts
    install_requires=['rfpipe', 'evla_mcast', 'sdmpy', 'click',
                      'elasticsearch', 'distributed', 'future'],
                      # 'rfgpu', 'vysmaw_reader'],
    entry_points='''
        [console_scripts]
        realfast=realfast.cli:cli
'''
)
