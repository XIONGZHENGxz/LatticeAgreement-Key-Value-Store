#! /bin/bash

pids=`ps -C java -o pid | grep -o -E '[0-9]+'`
	for pid in $pids; do 
		kill -9 $pid > /dev/null 2>&1
	done

