#!/bin/bash

tmp=${1}
setup=${2}
confJpaxos="config/jpaxos_config.txt"
confOthers="config/config.txt"
confMaster="config/masters.txt"

jarFile="target/LA.jar"
remoteDir="~/xiong/intern/evaluation"
max=1000
valLen=3
configFile="runtime_config.txt"
dir=`pwd`
source "$dir/bin/read.sh"

rm -f "results.csv"

## compile project to jar 
if [[ ${tmp} == "-p" ]]
then 
	mvn "-Dmaven.test.skip" "package"
fi

cd "./target"
jar=`find . -name *dependen*.jar`
echo $jar
if [[ -n "$jar" ]]
then
	mv $jar "LA.jar"
fi
cd "../"


declare -a masters 
readarray masters < $confMaster
#read experiments set up
experiments=`readFileNoComments $setup`
echo $experiments
for exp in $experiments; 
do 
	rm -f $configFile
	IFS=':' read -r -a expArr <<< "$exp"
	target="${expArr[0]}"
	numReplicas="${expArr[1]}"
	numReplica=$((numReplicas - 1))
	numThreads="${expArr[2]}"
	echo $numReplicas
	numOps="${expArr[3]}"
	readsRatio="${expArr[4]}"
	distribution="${expArr[5]}"
	failure="${expArr[6]}"

	#read configuration
	declare -a servers 
	if [ "$target" = "jpaxos" ]; then
		readarray servers < $confJpaxos
	else 
		readarray servers < $confOthers
	fi

	#generate configuration file
	for i in `seq 0 $((numReplica - 1))`; do 
		server=${servers[$i]}
		server=${server%$'\n'}
		echo $server >> "$configFile"
	done

	echo -n "${servers[$numReplica]}" >> "$configFile"

	for i in `seq 0 ${numReplica}`; do 
		master="${masters[$i]}"
		master=${master%$'\n'}
		ssh "xiong@${master}" "cd $remoteDir ; ./kill.sh; rm -f $configFile"  
	done 


	for i in `seq 0 ${numReplica}`; do 
		master="${masters[$i]}"
		master=${master%$'\n'}
		ssh "xiong@${master}" 'mkdir -p' $remoteDir
		ssh "xiong@${master}" 'mkdir -p' "${remoteDir}"
		scp "$configFile" "xiong@${master}:${remoteDir}"
		scp $jarFile "xiong@${master}:${remoteDir}"
		scp "bin/kill.sh" "xiong@${master}:${remoteDir}"
	done
	
	for i in `seq 0 ${numReplica}`; do 
		master="${masters[$i]}"
		master=${master%$'\n'}
		echo $master
		if [ $target == "crdt" ]; then 
			freq="${expArr[7]}"
			ssh "xiong@${master}" "cd $remoteDir ;  java -cp LA.jar la.crdt.CrdtServer $i 1000 $freq $configFile" &
		elif [ "$target" == "gla" ]; then 
			ssh "xiong@${master}" "cd $remoteDir ;  java -cp LA.jar la.gla.GlaServer $i $failure 1000 $configFile" &
		elif [ "$target" == "jpaxos" ]; then 
			echo "xiong@${master}" 
			ssh "xiong@${master}" "cd $remoteDir ;  java -cp LA.jar la.jpaxos.JpaxosServer $i 1000 $configFile" &
		elif [ "$target" == "mgla" ]; then 
			ssh "xiong@${master}" "cd $remoteDir ;  java -cp LA.jar la.mgla.MglaServer $i 1000 $configFile" &
		else	
			echo "invalid target: $target"
		fi
	done 

	#start client
	if [ $target == "crdt" ]; then 
		res=`java -cp $jarFile "la.crdt.CrdtClient" $numOps $max $valLen $distribution $readsRatio t $configFile $numThreads` 
	elif [ "$target" == "gla" ]; then 
		res=`java -cp $jarFile "la.gla.GlaClient" $numOps $max $valLen $distribution $readsRatio $configFile $numThreads` 
	elif [ "$target" == "jpaxos" ]; then 
		res=`java -cp $jarFile "la.jpaxos.JpaxosClient" $numOps $max $valLen $distribution $readsRatio $configFile $numThreads` 
	elif [ "$target" == "mgla" ]; then 
		res=`java -cp $jarFile "la.mgla.MglaClient" $numOps $max $valLen $distribution $readsRatio $configFile $numThreads` 
	else
		echo "invalid target"
	fi

	echo "result: $res"
	
	#shutdown servers
	for i in `seq 0 ${numReplica}`; do 
		master="${masters[$i]}"
		master=${master%$'\n'}
		ssh "xiong@${master}" "cd $remoteDir ; ./kill.sh ; rm -f $configFile"  
	done 


	#parse result
	arr=()
	while read -r line; do
		arr+=("$line")
	done <<< "$res"
	throughput="${arr[0]}"
	echo $throughput
	IFS=$'\n' read -ra ret <<< "$res"
	latency="${arr[1]}"
	echo $latency

	#write results to file
	if [ ! -f "results.csv" ]; then 
		echo "target,numReplicas,numThreads,numOps,readsRatio,distribution,throughput,latency" >> "results.csv"
	fi
	echo "${target},${numReplicas},${numThreads},${numOps},${readsRatio},${distribution},${throughput},${latency}" >> "results.csv"
done

