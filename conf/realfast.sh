#!/bin/bash
. ~/anaconda/etc/profile.d/conda.sh
conda activate deployment3
cd /lustre/evla/test/realfast
exec realfast run --mode deployment
