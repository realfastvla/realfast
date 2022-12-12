#!/usr/bin/env bash

MODE=$1
SDMNAME=$2
if [ $MODE == "create" ]; then
    ssh rfr "conda activate development; realfast buildsdm --indexprefix final --copybdf --sdmname $SDMNAME; rsync -rL --remove-source-files ${SDMNAME} claw@nmpost-master:~/fasttransients/realfast/tmp ; find ${SDMNAME} -type d -empty -delete"
elif [ $MODE == "ingest" ]; then
    ssh nmngas "activate_profile dsoc-test; realfastIngest -s /lustre/aoc/projects/fasttransients/realfast/tmp -p /lustre/aoc/projects/fasttransients/realfast/plots/final ${SDMNAME}"
else
    echo "Oops"
fi
