#!/usr/bin/env bash

SDMNAME=$1
PROFILE=$2  # can be dsoc-test or dsoc-prod

ssh rfr "conda activate deployment; cd lustre_workdir; realfast buildsdm --indexprefix final --sdmname "${SDMNAME}"; rsync -aL --bwlimit=20m --remove-source-files "${SDMNAME}" claw@nmpost-master:~/fasttransients/realfast/sdm_archive"

ssh nmngas "activate_profile "${PROFILE}"; realfastIngest -s /lustre/aoc/projects/fasttransients/realfast/sdm_archive -p /lustre/aoc/projects/fasttransients/realfast/plots/final "${SDMNAME}
