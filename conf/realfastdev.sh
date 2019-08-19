#!/bin/bash
. ~/anaconda/etc/profile.d/conda.sh
conda activate development36
cd /lustre/evla/test/realfast
exec realfast run --mode development
