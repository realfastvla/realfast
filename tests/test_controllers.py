import pytest
import os
from realfast import controllers

_install_dir = os.path.abspath(os.path.dirname(__file__))


# TODO:
# def test_controller_args
#
# def test_controller_none
#
# def test_controller_inmeta
#
# def test_readdata
#
# def test_candcollection
#
# def test_removefutures
#
# def test_throttle

def test_controller():
    rfc = controllers.realfast_controller(host='localhost',
                                          preffile=os.path.join(_install_dir, 'data/realfast.yml'),
                                          inprefs={'dmarr': [0], 'dtarr': [1]},
                                          indexresults=False,
                                          saveproducts=False,
                                          archiveproducts=False,
                                          throttle=False)

    rfc.handle_sdm(sdmfile=os.path.join(_install_dir,
                                        'data/16A-459_TEST_1hr_000.57633.66130137732.scan7.cut1'),
                   sdmscan=7)

    assert len(rfc.futures) > 0
