# realfast

Library and scripts for real-time transient search of data at the Very Large Array.
Designed to run on correlator backend (CBE, a compute cluster).
Goal is to detect incoming data, define fast transient search pipeline, and queue search jobs on nodes of cluster.

Requirements:
---
* Python 2.7 and scientific python stuff (numpy, etc.)
* [rtpipe](http://github.com/caseyjlaw/rtpipe) (and thus pwkit via anaconda installer)
* [rq](http://python-rq.org)
* [redis](http://redis.io)
* [supervisor](http://supervisord.org)
* [click](http://click.pocoo.org/)

Install
---
    python setup.py install
or
    pip install realfast

Contributors:
---

[Casey Law](http://www.twitter.com/caseyjlaw) -- UC Berkeley

Sarah Burke-Spolaor -- NRAO
