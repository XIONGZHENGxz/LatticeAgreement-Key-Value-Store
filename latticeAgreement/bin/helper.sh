#!/bin/bash

mvn "-Dmaven.test.skip" "package"
cd "./target"
jar=`find . -name *dependen*.jar`
echo $jar
mv $jar "Fusion.jar"
