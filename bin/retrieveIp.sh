#! /bin/bash

ips1=`aws ec2 describe-instances \
  --query "Reservations[*].Instances[*].PublicIpAddress" \
  	--filters "Name=instance-type,Values=t2.small" \
    --output=text`

ips2=`aws ec2 describe-instances \
  --query "Reservations[*].Instances[*].PublicIpAddress" \
  	--filters "Name=instance-type,Values=t2.micro" \
    --output=text`

config="config/config.txt"
jpaxos_config="config/jpaxos_config.txt"
masters="config/masters.txt"
rm -f $config
rm -f $jpaxos_config
rm -f $masters

i=0

for ip in $ips1; do
	echo $ip
	echo "${ip}" >> "config/masters.txt"
	echo "${ip}:18861:18862" >> "config/config.txt"
	echo "process.${i} = ${ip}:18861:18862" >> "config/jpaxos_config.txt"
	i=$((i + 1))
done

for ip in $ips2; do
	echo $ip
	echo "${ip}" >> "config/masters.txt"
	echo "${ip}:18861:18862" >> "config/config.txt"
	echo "process.${i} = ${ip}:18861:18862" >> "config/jpaxos_config.txt"
	i=$((i + 1))
done
