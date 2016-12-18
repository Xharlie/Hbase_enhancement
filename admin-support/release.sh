#!/usr/bin/env bash

# Make a tarball for release modules of hbase.

PREV_DIR=$(pwd);
echo "PREV_DIR: [${PREV_DIR}]";
BASE_DIR=$(dirname "${BASH_SOURCE-$0}");
BASE_DIR=$(cd "${BASE_DIR}" >/dev/null; pwd);
echo "BASE_DIR: [${BASE_DIR}]";
cd ${BASE_DIR}/..;
TMP_DIR=./RELEASE_HBASE_$(date +%Y%m%d);
rm -rf ${TMP_DIR};

MODULES=(
    hbase-annotations 
    hbase-checkstyle
    hbase-client
    hbase-common
    hbase-examples
    hbase-hadoop2-compat
    hbase-hadoop-compat
    hbase-it
    hbase-prefix-tree
    hbase-procedure
    hbase-protocol
    hbase-resource-bundle
    hbase-rest
    hbase-server
    hbase-shell
    hbase-testing-util
    hbase-thrift);
for m in "${MODULES[@]}"; do
    echo "Copying $m ...";
    mkdir -p ${TMP_DIR}/$m;
    cp $m/target/$m*.jar ${TMP_DIR}/$m/;
    cp $m/pom.xml ${TMP_DIR}/$m/;
done
cp ./pom.xml ${TMP_DIR}/;

echo "Make tarball...";
rm -f hbase-release.tar.gz
tar zcf ./hbase-release.tar.gz ${TMP_DIR};
rm -rf ${TMP_DIR};

echo "Done!";
cd ${PREV_DIR};
exit 0;
