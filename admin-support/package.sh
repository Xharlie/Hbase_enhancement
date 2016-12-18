#!/usr/bin/env bash

COMPILE_JDK=/home/hadoop/hadoop_hbase/jdk-current;
test -e ${COMPILE_JDK};
if [ $? -eq 0 ]; then
    export JAVA_HOME=${COMPILE_JDK};
fi
mvn clean package -DskipTests assembly:single;

# recreate tarball in target dir
echo ""
echo "------------------------------------------------------------------------"
echo "Clean up, copy necessary lib jars and recreate tarball..."
echo "------------------------------------------------------------------------"
CURRENT_DIR=`dirname $0`
CURRENT_DIR=`cd $CURRENT_DIR;pwd`
BUILD_DIR=`cd $CURRENT_DIR/../hbase-assembly/target;pwd`
cd $BUILD_DIR
PACKAGE=`ls | grep tar`
DECOMPRESS_DIR=${PACKAGE%\-*}
tar zxf ${PACKAGE}
rm -fv ./${DECOMPRESS_DIR}/bin/*.cmd
rm -fv ./${DECOMPRESS_DIR}/conf/*.cmd
rm -fv ./${DECOMPRESS_DIR}/lib/hadoop-*.jar
rm -fv ./${DECOMPRESS_DIR}/lib/hqueue-*.jar
# we assume current dir is "admin-support" here
cp -rv ${CURRENT_DIR}/lib/*.jar ./${DECOMPRESS_DIR}/lib/
rm -fv ${PACKAGE}
tar zcf ${PACKAGE} ./${DECOMPRESS_DIR}
ls -l ${PACKAGE}
rm -rf ./${DECOMPRESS_DIR}
echo "------------------------------------------------------------------------"
echo "DONE."
echo "------------------------------------------------------------------------"

exit 0;
