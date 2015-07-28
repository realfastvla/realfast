#!/usr/bin/env bash
# commands to start up redis and rqworkers on nodes

# set rq parameter file
export host=`hostname`

# start redis
if [ "$1" = 'start' ]; then
    echo 'Starting redis server'
    redis-server ~claw/code/realfast/conf/redis_${host}.conf  # **fix**

# start rqworkers
    nworkers=1
    for nodename in ${@:2}; do
	echo 'Starting '$nworkers' rq workers on '$nodename
#	for i in $(seq 1 $nworkers); do
#	for name in joblists cleanup plot cal search ; do
	for name in default ; do
	    ssh $nodename screen -d -m -S $name rq worker $name -u redis://$host
	done
    done
fi

if [ "$1" = 'stop' ]; then
# kill rqworkers
#    for nodename in ${@:2}; do
#	echo 'Stopping rq workers on '$nodename
#	ssh $nodename pkill rqworker 2> /dev/null
#    done

# stop server
    echo 'Stopping redis server'
    redis-cli shutdown
fi