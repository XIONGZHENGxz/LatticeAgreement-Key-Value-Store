#! /bin/bash
key="xiong-key-pair.pem"
scp -i $key "${username}@${master}:${remoteDir}/results.csv" .
