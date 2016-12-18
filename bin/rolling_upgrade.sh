#!/bin/bash

usage(){
    echo "Usage: $0 <hostFile> <num> <version> <time>";
    echo "  hostFile     File with hosts-per-line to upgrade";
    echo "  num          thread num";
    echo "  version      Target version it would upgrade to";
    echo "  time         Sleep time default 900";
    exit 1;
}

if [ $# -le 2 ]; then
    usage
fi

upgradefile=$1
num=$2
upversion=$3
if [ "x$4" == "x" ];
then
    sleepTime=900
else
    sleepTime=$4
fi

count="$(cat $upgradefile|wc -l)"
if [ $count -gt 0 ]
then
    if [ $num -gt 1 ]
    then
        rcount=`echo "($count-1)/$num+1"|bc`
    elif [ $num -eq 1 ]
    then
        rcount=$count
    else
        echo "The num must greater than 0 and must be int"
        usage
    fi
else
    echo "the grace update regionserver's number must greater than 0"
    usage
fi

bin=$(dirname "$0");
bin=$(cd "$bin" >/dev/null; pwd);
. "$bin"/hbase-config.sh;

version=$upversion;
excludeFile=$upgradefile

echo "upgrade file is $upgradefile, the number of regionserver is: $count"
echo "upgrade thread num is $num"
echo "upgrade version is $version"
echo "rolling's count is $rcount"
echo "sleep time is $sleepTime"

echo "sleep 10s ..."
sleep 10

date;
echo "Disabling balancer ...";
HBASE_BALANCER_STATE=$(echo 'balance_switch false' | "$bin"/hbase --config ${HBASE_CONF_DIR} shell | tail -n 3 | head -n 1);
echo "Previous balancer state was ${HBASE_BALANCER_STATE}";

for ((i=1;i<=$rcount;i++));
do
    begin=`echo "($i-1)*$num+1"|bc`
    end=`echo $i*$num|bc`
    sed -n ${begin},${end}p $upgradefile > "${upgradefile}.$i"
    hostFile=${upgradefile}.$i
    echo "start rolling regionservers:"
    cat $hostFile
    echo "sleep 10s ..."
    sleep 10
    
    for node in $(cat $hostFile); do
        echo "Start upgrading $node ...";
        "$bin"/graceful_upgrade.sh $node $version $excludeFile >>/tmp/hbase_upgrade_$node.log 2>&1 &
    done
    
    if [ $i -lt $rcount ]
    then
        echo "Waiting for this subprocesses, sleep: ${sleepTime}s ...";
        sleep $sleepTime
    fi

done

echo "Waiting for all subprocesses ...";
wait

if [ ${HBASE_BALANCER_STATE} != "false" ]; then
    echo "Restoring balancer state to ${HBASE_BALANCER_STATE} ...";
    echo "balance_switch ${HBASE_BALANCER_STATE}" | "$bin"/hbase --config ${HBASE_CONF_DIR} shell &>/dev/null
else
    echo "Do not need to restore balancer switch state, because the original value was false.";
fi

echo "All done!";
exit 0;
