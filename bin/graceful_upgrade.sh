#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 <hostname> <version> <excludeFile>";
    echo "  hostname     Hostname of server we are to upgrade";
    echo "  version      Target version it would upgrade to";
    echo "  excludeFile  File with hosts-per-line to exclude as unload targets";
    exit 1;
fi

bin=$(dirname "$0");
bin=$(cd "$bin" >/dev/null; pwd);
. "$bin"/hbase-config.sh;

hostname=$1;
version=$2;
excludeFile=$3;
filename="/tmp/$hostname";

date;
echo "Unloading $hostname region(s)";
HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} org.jruby.Main "$bin"/region_mover.rb --file=$filename --debug --excludefile=$excludeFile unload $hostname
echo "Unloaded $hostname region(s)";

hosts="/tmp/$(basename $0).$$.tmp";
echo $hostname >>$hosts;
"$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts $hosts stop regionserver

#at least wait 10s after stop the rs
sleep 10

RELINK=$(ssh $hostname "test -d /home/hadoop/hadoop_hbase/hbase-$version; echo \$?");
if [ $RELINK -eq 0 ]; then
    echo "Upgrade regionserver of [$hostname] to version: $version";
    ssh $hostname "cd /home/hadoop/hadoop_hbase/; ln -nfs ./hbase-$version ./hbase-current; ls -l ./hbase-current"; 
else
    echo "Warning: Cannot relink to /home/hadoop/hadoop_hbase/hbase-$version, so just restart!";
fi

"$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts $hosts start regionserver
rm -f $hosts;
sleep 5;

echo "Reloading $hostname region(s)";
HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} org.jruby.Main "$bin"/region_mover.rb --file=$filename --debug load $hostname
echo "Reloaded $hostname region(s)";

exit 0;
