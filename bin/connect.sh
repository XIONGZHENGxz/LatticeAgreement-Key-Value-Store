#! /bin/bash

declare -a masters 
readarray masters < "config/masters.txt" 

master="${masters[${1}]}"
master=${master%$'\n'}
ssh -i "xiong-key-pair.pem" "ubuntu@${master}" 
