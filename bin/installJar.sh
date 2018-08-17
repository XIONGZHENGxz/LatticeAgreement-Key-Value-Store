#! /bin/bash

mvn install:install-file -Dfile=jpaxos.jar -DgroupId=lsr -DartifactId=jpaxos -Dversion=1.0 -Dpackaging=jar
