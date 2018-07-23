#!/bin/bash

jarFile="target/LA.jar"
remoteDir="~/xiong/intern/evaluation"
max=1000
valLen=3
configFile="runtime_config.txt"

declare -a masters 
readarray masters < ${2} 
echo $masters 

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

for i in `seq 0 ${#masters[@]}`; do
	master="${masters[$i]}"
	master=${master%$'\n'}
	echo $master
	scp "$configFile" "xiong@${master}:${remoteDir}"
	scp $jarFile "xiong@${master}:${remoteDir}"
done

