#!/usr/bin/env bash

COMPILE_JDK=/home/hadoop/hadoop_hbase/jdk-current;
test -e ${COMPILE_JDK};
if [ $? -eq 0 ]; then
    export JAVA_HOME=${COMPILE_JDK};
fi
mvn -s $HOME/.m2/settings.xml \
    -PrunAllTests \
    -Dmaven.test.redirectTestOutputToFile=true\
    -Dit.test=noItTest\
    -Dhbase.skip-jacoco=false\
    -Dmaven.test.failure.ignore=true \
    -Dsurefire.testFailureIgnore=true \
     clean test 

exit 0;
