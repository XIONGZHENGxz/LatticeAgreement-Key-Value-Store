#! /bin/bash

ips=`aws ec2 describe-instances \
  --query "Reservations[*].Instances[*].PublicIpAddress" \
    --output=text`
config="config/config.txt"
jpaxos_config="config/jpaxos_config.txt"
masters="config/masters.txt"
rm -f $config
rm -f $jpaxos_config
rm -f $masters
i=0

for ip in $ips; do
	echo $ip
	echo "${ip}" >> "config/masters.txt"
	echo "${ip}:18861:18862" >> "config/config.txt"
	echo "process.${i} = ${ip}:18861:18862" >> "config/jpaxos_config.txt"
	i=$((i + 1))
done
