# realfast

Realfast is the name of a project and software package related to radio interferometric data analysis at the Very Large Array. For more information, see [realfast.io](http://realfast.io) or visit the [VLA](https://public.nrao.edu/telescopes/vla/) web site.

This repo includes a Python 2 application that integrates a transient search pipeline with the real-time environment at the VLA. This requires running asynchronous processes in a cluster environment and using VLA services for distributing data, metadata, and archiving. This application:
* monitors multicast messages with [evla_mcast](https://github.com/demorest/evla_mcast),
* catches visibility data via the [vys](https://github.com/mpokorny/vysmaw) protocol,
* defines a fast transient search pipeline with [rfpipe](http://github.com/realfastvla/rfpipe),
* submits search pipeline to a compute cluster with [distributed](https://github.com/dask/distributed),
* indexes candidate transients and other metadata for the [search interface](https://github.com/realfastvla/realfast.io-search), and
* writes and archives new visibility files for candidate transients.

These are the core functions required for the realfast project. Other features include:
* Support for GPU algorithms,
* Management of distributed futures,
* Blind injection and management of mock transients, and
* Reading of sdm data via [sdmpy](https://github.com/demorest/sdmpy).

Requirements:
---------
The realfast application is build upon the following libraries:

* Python 2.7 and the scientific python stack (numpy, etc.)
* [rfpipe](http://github.com/realfastvla/rfpipe)
* [rtpipe](http://github.com/caseyjlaw/rtpipe) (temporarily required for its flagger)
* [evla_mcast](https://github.com/demorest/evla_mcast)
* [vys](https://github.com/mpokorny/vysmaw) and [vysmaw_apps](https://github.com/realfastvla/vysmaw_apps)
* [distributed](https://github.com/dask/distributed)
* [pyfft](https://pythonhosted.org/pyfft) and [pycuda](https://mathema.tician.de/software/pycuda) for GPU support
* [elasticsearch](https://github.com/elastic/elasticsearch-py),
* [supervisor](http://supervisord.org) (to manage/daemonize processes)
* [click](http://click.pocoo.org)
* [sdmpy](https://github.com/demorest/sdmpy) (optional, to read/write sdm/bdf data)
 
Install
------
This application is designed to integrate specific protocols and services only availble on the correlator cluster at the VLA. If you'd like to build it yourself, you will need to use the [anaconda](http://anaconda.com) installer and follow instructions at [rfpipe](http://github.com/realfastvla/rfpipe). Then install the dependencies above (vys and pycuda being the trickiest) and:

    python setup.py install

Contributors:
--------
The custom realfast software includes contributions from:

* Sarah Burke-Spolaor -- WVU
* Paul Demorest -- NRAO
* Andrew Halle -- UC Berkeley
* Casey Law -- UC Berkeley
* Joe Lazio -- JPL
* Martin Pokorny -- NRAO
