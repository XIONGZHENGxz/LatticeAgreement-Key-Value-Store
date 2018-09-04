#!/bin/bash

jarFile="target/LA.jar"
remoteDir="~/latticeAgreement"
confJpaxos="config/jpaxos_config.txt"
config="config/config.txt"
confMaster="config/masters.txt"
max=1000
valLen=3
configFile="runtime_config.txt"
username="ubuntu"
keyFile="xiong-key-pair.pem"

declare -a masters 
readarray masters < $confMaster 

if [[ ${1} == "-p" ]]
then 
	mvn "-Dmaven.test.skip" "package"
fi

cd "./target"
jar=`find . -name *dependen*.jar`
if [[ -n "$jar" ]]
then
	mv $jar "LA.jar"
fi
cd "../"
num=${#masters[@]}
num=$((num - 1))
for i in `seq 0 ${num}`; do
	master="${masters[$i]}"
	master=${master%$'\n'}
	echo $master
	if [[ ${2} == "-n" ]]; then
		echo -e "yes" | ssh -i $keyFile "${username}@${master}" "cd ~/; rm "${remoteDir}"; mkdir -p "${remoteDir}""
	scp -i $keyFile $config "${username}@${master}:${remoteDir}"
	scp -i $keyFile $confJpaxos "${username}@${master}:${remoteDir}"
	scp -i $keyFile $confMaster "${username}@${master}:${remoteDir}"
	scp -i $keyFile "bin/kill.sh" "${username}@${master}:${remoteDir}"
	fi
	if [[ ${1} == "-p" ]]; then
		scp -i $keyFile $jarFile "${username}@${master}:${remoteDir}"
	fi
	if [[ ${2} == "-n" ]];then
		scp -i $keyFile $keyFile "${username}@${master}:${remoteDir}"
		ssh -i $keyFile "${username}@${master}" "sudo apt update; sudo apt install openjdk-8-jdk -y"
	fi

done

master="${masters[${num}]}"
master=${master%$'\n'}
scp -i $keyFile "bin/build.sh" "${username}@${master}:${remoteDir}"
if [[ "${2}" == "-c" || "${2}" == "-n" ]];then
	scp -i $keyFile -r "config/" "${username}@${master}:${remoteDir}"
	scp -i $keyFile -r "bin/" "${username}@${master}:${remoteDir}"
fi
