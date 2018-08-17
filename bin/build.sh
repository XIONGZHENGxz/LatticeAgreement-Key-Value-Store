#!/bin/bash

setup=${1}
confJpaxos="config/jpaxos_config.txt"
confOthers="config/config.txt"
confMaster="config/masters.txt"
numClients=2

jarFile="LA.jar"
remoteDir="~/${username}/latticeAgreement"
max=1000
valLen=3
configFile="runtime_config.txt"
username="ubuntu"
dir=`pwd`
keyFile="xiong-key-pair.pem"
source "$dir/bin/read.sh"

rm -f "results.csv"
declare -a masters 
readarray masters < $confMaster
numMaster=${#masters[@]}
num=$((numMaster - 2))
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
	numOps="${expArr[3]}"
	readsRatio="${expArr[4]}"
	distribution="${expArr[5]}"
	failure="${expArr[6]}"
	fail="${expArr[7]}"
	numProp="${expArr[8]}"

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

	for i in `seq 0 ${num}`; do 
		master="${masters[$i]}"
		master=${master%$'\n'}
		ssh -i $keyFile "${username}@${master}" "cd $remoteDir ; ./kill.sh; rm -f $configFile"  
	done 


	for i in `seq 0 ${num}`; do 
		master="${masters[$i]}"
		master=${master%$'\n'}
		ssh -i $keyFile "${username}@${master}" 'mkdir -p' $remoteDir
		ssh -i $keyFile "${username}@${master}" 'mkdir -p' "${remoteDir}"
		scp -i $keyFile "$configFile" "${username}@${master}:${remoteDir}"
		scp -i $keyFile "bin/kill.sh" "${username}@${master}:${remoteDir}"
	done
	
	for i in `seq 0 ${numReplica}`; do 
		master="${masters[$i]}"
		master=${master%$'\n'}
		echo $master
		if [ $target == "crdt" ]; then 
			freq="${expArr[9]}"
			ssh -i $keyFile "${username}@${master}" "cd $remoteDir ;  java -cp LA.jar la.crdt.CrdtServer $i 1000 $freq $configFile" &
		elif [ "$target" == "gla" ]; then 
			ssh -i $keyFile "${username}@${master}" "cd $remoteDir ;  java -cp LA.jar la.gla.GlaServer $i $failure 1000 $configFile" &
		elif [ "$target" == "jpaxos" ]; then 
			echo "${username}@${master}" 
			ssh -i $keyFile "${username}@${master}" "cd $remoteDir ;  java -cp LA.jar la.jpaxos.JpaxosServer $i 1000 $configFile" &
		elif [ "$target" == "mgla" ]; then 
			ssh -i $keyFile "${username}@${master}" "cd $remoteDir ;  java -cp LA.jar la.mgla.MglaServer $i 1000 $configFile" &
		elif [ "$target" == "wgla" ]; then 
			ssh -i $keyFile "${username}@${master}" "cd $remoteDir ;  java -cp LA.jar la.wgla.GlaServer $i $failure 1000 $configFile $fail" &
		else	
			echo "invalid target: $target"
		fi
	done 
	
	sleep 1
	#start clients
	master="${masters[$num]}"
	master=${master%$'\n'}
	if [ $target == "crdt" ]; then 
		ssh -i $keyFile "${username}@${master}" "cd $remoteDir ; java -cp $jarFile la.crdt.CrdtClient $numOps $max $valLen $distribution $readsRatio t $configFile $numThreads $numProp" & 
	elif [ "$target" == "gla" ] || [ "$target" == "pgla" ] || [ "$target" == "wgla" ]; then 
		ssh -i $keyFile "${username}@${master}" "cd $remoteDir ; java -cp $jarFile la.gla.GlaClient $numOps $max $valLen $distribution $readsRatio $configFile $numThreads $numProp" &
	elif [ "$target" == "jpaxos" ]; then 
		ssh -i $keyFile "${username}@${master}" "cd $remoteDir ; java -cp $jarFile la.jpaxos.JpaxosClient $numOps $max $valLen $distribution $readsRatio $configFile $numThreads $numClients" & 
	elif [ "$target" == "mgla" ]; then 
		ssh -i $keyFile "${username}@${master}" "cd $remoteDir ; java -cp $jarFile la.mgla.MglaClient $numOps $max $valLen $distribution $readsRatio $configFile $numThreads $numProp" &
	else
		echo "invalid target"
	fi

	if [ $target == "crdt" ]; then 
		res=`java -cp $jarFile la.crdt.CrdtClient $numOps $max $valLen $distribution $readsRatio t $configFile $numThreads $numProp $numClients` 
	elif [ "$target" == "gla" ] || [ "$target" == "pgla" ] || [ "$target" == "wgla" ]; then 
		res=`java -cp $jarFile la.gla.GlaClient $numOps $max $valLen $distribution $readsRatio $configFile $numThreads $numProp $numClients` 
	elif [ "$target" == "jpaxos" ]; then 
		res=`java -cp $jarFile la.jpaxos.JpaxosClient $numOps $max $valLen $distribution $readsRatio $configFile $numThreads $numProp $numClients` 
	elif [ "$target" == "mgla" ]; then 
		res=`java -cp $jarFile la.mgla.MglaClient $numOps $max $valLen $distribution $readsRatio $configFile $numThreads $numProp $numClients`
	else
		echo "invalid target"
	fi
	
	echo "result: $res"
	
	#shutdown servers
	for i in `seq 0 ${num}`; do 
		master="${masters[$i]}"
		master=${master%$'\n'}
		ssh -i $keyFile "${username}@${master}" "cd $remoteDir ; ./kill.sh ; rm -f $configFile"  
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

