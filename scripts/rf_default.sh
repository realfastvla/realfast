#!/bin/sh
# Realfast setup defaults
export DO_ARCH="True"
export BDF_ARCHDIR="/home/cbe-master/realfast/fake_archdir"
export SDM_ARCHDIR="/home/cbe-master/realfast/fake_archdir"
export WORKDIR="/home/cbe-master/realfast/workdir"
# Arguments to mcaf_monitor.py
export MCAF_MON_NICE="10"
export MCAF_INPUTS="-v --do --rtparams /home/cbe-master/realfast/workdir/rtpipe_cbe.conf"
