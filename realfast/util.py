from __future__ import print_function, division, absolute_import #, unicode_literals # not casa compatible
from builtins import bytes, dict, object, range, map, input#, str # not casa compatible
from future.utils import itervalues, viewitems, iteritems, listvalues, listitems
from io import open

import numpy as np
import logging
logger = logging.getLogger(__name__)


def data_logging_report(filename):
    """ Read and summarize data logging file
    """
    with open(filename, 'r') as fp:
        for line in fp.readlines():
            segment, starttime, dts, filled = line.split(' ')
            segment = int(segment.rstrip(':'))
            starttime = float(starttime)
            dts = [float(dt) for dt in dts.split(',')]
            filled = [fill.rstrip() == "True" for fill in filled.split(',')]
            if len(filled) > 1:
                filled = np.array(filled, dtype=int)
                logger.info("Segment {0}: {1}/{2} missing"
                            .format(segment, len(filled)-filled.sum(),
                                    len(filled)))
                logger.info("{0}".format(filled))
            else:
                logger.info("Segment {0}: no data".format(segment))
