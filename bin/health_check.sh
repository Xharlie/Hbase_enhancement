#!/bin/bash

# handle SIGTERM
trap "/bin/true" TERM

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# This will set HBASE_HOME, etc.
. "$bin"/hbase-config.sh

if [ $# != 5 ]; then
  echo "health check shell script should have at list 5 arguments."
  echo "Usage: health_check.sh <thread_pool_size> <failed_threshold> <sampled_region_count> <scan_timeout> <server_name>"
  echo "Example health_check.sh 10 0.2 10 60 hadoop0031.su18.tbsite.net,16020,1469598316613"
  echo "thread_pool_size, the thread size that will do parallel scan, should be int and between [0,30]."
  echo "failed_threshold, if scan failed region percent > this value, probe failed. should be float and between [0,1]"
  echo "sampled_region_count, how many online regions will be selected to scan/probe.should be int and greater than 0."
  echo "scan_timeout, scan timeout time, unit is microsecond, should be int and greater than 0."
  echo "server_name, rs that will do health check, should be of format "hadoop0031.su18.tbsite.net,16020,1469598316613", for example."
  exit 3
fi

thread_pool_size=$1
failed_threshold=$2
sampled_region_count=$3
scan_timeout=$4
server_name=$5

hbase_classpath=`"$bin"/hbase classpath`
exit_code=0

health_check_log_file_name=${HBASE_LOG_DIR}/health_check.log
health_check_log_file_size=`ls -l ${health_check_log_file_name} | awk '{ print $5}'`
file_size_upper_limit=100000000
if [ ${health_check_log_file_size} -gt ${file_size_upper_limit} ]
then
  rm -f ${health_check_log_file_name}
fi

echo -e "\n\ncurrent date : `date`" >> ${health_check_log_file_name}
${JAVA_HOME}/bin/java -Xmx5g -cp ${hbase_classpath} org.apache.hadoop.hbase.tool.HealthCheck $thread_pool_size $failed_threshold $sampled_region_count $scan_timeout $server_name >> ${health_check_log_file_name}
if [ $? != 0 ]
then
  exit_code=1
else
  exit_code=0
fi
echo -e "current date : `date`" >> ${health_check_log_file_name}
echo "exit code is ${exit_code}" >> ${health_check_log_file_name}
exit ${exit_code}
