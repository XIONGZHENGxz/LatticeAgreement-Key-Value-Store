#!/bin/bash

function readFile {
	while IFS=':' read -ra master;do
		masters+=\ $master
	done <$1
	echo $masters
}

function readConf {
	declare -a servers
	readarray servers < $1
	echo $servers
}

function readFileNoComments {
	while read -r line; do
		[[ "$line" =~ ^#.*$ ]] && continue
		echo $line
		sets+=($line)
	done <$1
}
