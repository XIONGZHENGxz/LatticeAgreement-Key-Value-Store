#! /bin/bash

###config to log in servers without password

declare -a masters
readarray masters < "config/masters.txt"

ssh-keygen -t rsa

for i in `seq 0 ${#masters[@]}`; do
	master=${masters[$i]}
	master=${master%$'\n'}
	ssh "xiong@${master}" mkdir -p ~/.ssh
	cat ~/.ssh/id_rsa.pub | ssh "xiong@${master}" 'cat >> ~/.ssh/authorized_keys'
done 

ssh-add



