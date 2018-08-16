#!/bin/bash

jarFile="target/KVStore.jar"
remoteDir="~/ds/fusion/"
inputFile="input.json"
conf="conf/masters"
configFusion="config.json"
configZoo="configZoo.json"
masters=()
zookeeper="uta.shan.zookeeperBasedDS.Client"
fusion="uta.shan.fusionBasedDS.Client"
fusionPrimary="uta.shan.fusionBasedDS.PrimaryServer"
fusionBackup="uta.shan.fusionBasedDS.FusedServer"

function readFile {
	while read user;do
		read host
		master=$user@$host 
		masters+=\ $master
	done <${1} 
}
readFile $conf


if [[ ${1} == '-z' ]]
then 
	
	#for master in $masters
	#do 
		#ssh $master | 'zkServer.sh start' 
		#ssh $master | 'zkServer.sh status'
	#done
	#zkServer.sh start
	#zkServer.sh status
	java -cp $jarFile $zookeeper $configZoo $inputFile	

elif [[ ${1} == '-f' ]]
then
	#i=0
	#for master in $masters
	#do 
	#	ssh $master | java -cp '$jarFile $fusionPrimary $configFusion' $i  
	#	i=$((i+1))
	#done
	java -cp $jarFile $fusion $configFusion $inputFile

else 
	echo "usage ./Client.sh [-z|-f]"
fi
