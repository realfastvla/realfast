#!/usr/bin/env bash

SDMNAME=$1
PROFILE=$2  # can be dsoc-test or dsoc-prod

ssh rfr "conda activate deployment; cd lustre_workdir; output=\$(realfast buildsdm --indexprefix final --sdmname ${SDMNAME}); if echo '$output' | grep -q 'No bdf found'; then echo 'No BDF found'; else ~/soft/sdmpy/scripts/realfast_sdm_fix.py "${SDMNAME}"; fi; rsync -arL --ignore-existing --bwlimit=20m --remove-source-files "${SDMNAME}".fix/ claw@nmpost-master:~/fasttransients/realfast/sdm_archive/"${SDMNAME}

#ssh rfr "conda activate deployment; cd lustre_workdir; realfast buildsdm --indexprefix final --sdmname "${SDMNAME}"; ~/soft/sdmpy/scripts/realfast_sdm_fix.py "${SDMNAME}"; rsync -arL --ignore-existing --bwlimit=20m --remove-source-files "${SDMNAME}".fix/ claw@nmpost-master:~/fasttransients/realfast/sdm_archive/"${SDMNAME}

ssh nmngas "activate_profile "${PROFILE}"; realfastIngest -s /lustre/aoc/projects/fasttransients/realfast/sdm_archive -p /lustre/aoc/projects/fasttransients/realfast/plots/final "${SDMNAME}
