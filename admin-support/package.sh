#!/usr/bin/env bash

COMPILE_JDK=/home/hadoop/hadoop_hbase/jdk-current;
test -e ${COMPILE_JDK};
if [ $? -eq 0 ]; then
    export JAVA_HOME=${COMPILE_JDK};
fi
mvn clean package -DskipTests assembly:single;

exit 0;
