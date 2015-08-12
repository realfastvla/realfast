# realfast

Library and scripts for real-time transient search of data at the Very Large Array.
Designed to run on correlator backend (CBE, a compute cluster).
Goal is to detect incoming data, define fast transient search pipeline, and queue search jobs on nodes of cluster.

Requirements:
---
* Python 2.7 and scientific python stuff (numpy, etc.)
* [rtpipe](http://github.com/caseyjlaw/rtpipe)
* [rq](http://python-rq.org)
* [redis](http://redis.io)
* [supervisor](http://supervisord.org)
* [pwkit](http://github.com/pkgw/pwkit) (used for CASA library access)
* [pyfftw](https://pypi.python.org/pypi/pyFFTW) (optional, accelerated FFTs)
* [sdmreader](http://github.com/caseyjlaw/sdmreader) (optional, for reading SDM format data from VLA archive)
* [sdmpy](http://github.com/caseyjlaw/sdmpy) (optional, for reading SDM format data from VLA archive)

Contributors:
---

[Casey Law](http://www.twitter.com/caseyjlaw) -- UC Berkeley

Sarah Burke-Spolaor -- NRAO
