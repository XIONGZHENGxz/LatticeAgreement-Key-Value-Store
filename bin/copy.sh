#! /bin/bash

key="xiong-key-pair.pem"
username="ubuntu"
confMaster="config/masters.txt"
remoteDir="~/latticeAgreement"
declare -a masters 
readarray masters < $confMaster 

num=${1}
	master="${masters[$num]}"
	master=${master%$'\n'}
scp -i $key "${username}@${master}:${remoteDir}/${2}" .
