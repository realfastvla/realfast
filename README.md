<img src=https://github.com/realfastvla/realfast/blob/gh-pages/images/realfast_black.png width=70% align="middle">

# realfast

realfast is name of a project and software package related to radio interferometric data analysis at the Very Large Array. This repo includes a library and scripts to run real-time data analysis on the correlator backend (CBE, a compute cluster). For info on the project, please see http://realfast.io and [rfpipe](http://github.com/realfastvla/rfpipe).

The goal of this software is to detect incoming data, define fast transient search pipeline, and queue search jobs on nodes of cluster. Workers are running the transient search pipeline called [rtpipe](http://github.com/caseyjlaw/rtpipe).

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

Logo:
---
[Studio Principle](https://www.studioprinciple.com)
