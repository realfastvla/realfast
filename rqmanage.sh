#!/usr/bin/env bash
# commands to start up redis and rqworkers on nodes

# set rq parameter file
export host=`hostname`
if [ "$host" == 'gygax' ]; then export rqsettings='rqsettings_aoc'; fi
if [ "$host" == 'cbe-master' ]; then export rqsettings='rqsettings_cbe'; fi

# start redis
if [ "$1" = 'start' ]; then
    echo 'Starting redis server'
    redis-server ~claw/code/realfast/redis.conf

# start rqworkers
    nworkers=1
    for nodename in ${@:2}; do
	echo 'Starting '$nworkers' rqworkers on '$nodename
#	for i in $(seq 1 $nworkers); do
#	for name in joblists cleanup plot cal search ; do
	for name in default ; do
	    ssh $nodename screen -d -m -S $name rqworker $name --config $rqsettings
	done
    done
fi

if [ "$1" = 'stop' ]; then
# kill rqworkers
    for nodename in ${@:2}; do
	echo 'Stopping rqworkers on '$nodename
	ssh $nodename pkill rqworker 2> /dev/null
    done

# stop server
    echo 'Stopping redis server'
    redis-cli shutdown
fi