#!/usr/bin/env bash
# commands to start up redis and rqworkers on nodes
# run on gygax for now

# start redis
if [ "$1" = 'start' ]; then
    echo 'Starting redis server'
    redis-server ~claw/code/redis-stable/redis.conf

    nworkers=1
# start rqworkers
    for nodename in ${@:2}; do
	echo 'Starting '$nworkers' rqworkers on '$nodename
#	$logname='nohup_'${nodename}'.log'
	for i in $(seq 1 $nworkers); do
	    ssh $nodename "nohup rqworker -c rqsettings > nohup_$nodename.log 2>&1 &"
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