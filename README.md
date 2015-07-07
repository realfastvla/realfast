# realfast

Tools for real-time transient search of data at the Very Large Array
Designed to run on CBE (correlator cluster). 
Goal is to detect incoming data, define fast transient search pipeline, and queue search jobs on nodes of cluster.

Requirements:
---
* Python 2.7 and scientific python stuff (numpy, etc.)
* rtpipe (http://github.com/caseyjlaw/rtpipe)
* pwkit (for CASA access; http://github.com/pkgw/pwkit)
* pyfftw (https://pypi.python.org/pypi/pyFFTW)
* sdmreader (http://github.com/caseyjlaw/sdmreader)
* sdmpy (http://github.com/caseyjlaw/sdmpy)
* rq and redis (http://python-rq.org)
