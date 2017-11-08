import pytest
import os
from realfast import controllers

_install_dir = os.path.abspath(os.path.dirname(__file__))


def test_controller():
    rfc = controllers.realfast_controller(preffile=os.path.join(_install_dir, 'data/realfast.yml'),
                                          inprefs={'dmarr':[0], 'dtarr':[1]},
                                          indexresults=False,
                                          saveproducts=False,
                                          archiveproducts=False)

    rfc.handle_sdm(sdmfile=os.path.join(_install_dir,
                                        'data/16A-459_TEST_1hr_000.57633.66130137732.scan7.cut1'),
                   sdmscan=7)

    assert len(rfc.futures) > 0
