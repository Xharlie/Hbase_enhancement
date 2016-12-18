#!/usr/bin/env bash

# Modify the version of current hqueue pom(s).

PREV_DIR=$(pwd);
echo "PREV_DIR: [${PREV_DIR}]";
BASE_DIR=$(dirname "${BASH_SOURCE-$0}");
BASE_DIR=$(cd "${BASE_DIR}" >/dev/null; pwd);
echo "BASE_DIR: [${BASE_DIR}]";
cd ${BASE_DIR}/..;

function usage {
    echo "Usage: $0 CURRENT_VERSION NEW_VERSION";
    echo "For example, $0 0.4.2 0.4.2-SNAPSHOT";
    cd ${PREV_DIR};
    exit 1;
}

if [[ "$#" -ne 2 ]]; then usage; fi
old_hbase_version="$1";
new_hbase_version="$2";

pom="pom.xml";
poms=$(find . -name $pom);
for p in $poms; do
    echo "Update: [$p]";
    sed -i "s/<version>${old_hbase_version}<\/version>/<version>${new_hbase_version}<\/version>/g" $p;
done

echo "Done!";
cd ${PREV_DIR};
exit 0;
