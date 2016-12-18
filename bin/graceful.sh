#!/usr/bin/env bash
#
#/**
# * Copyright 2011 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Move regions off a server then stop it.  Optionally restart and reload.
# Turn off the balancer before running this script.
function usage {
  echo "Usage: graceful.sh [--config <conf-dir>] [--restart] [--start] [--stop]"
  echo " restart     If we should restart after graceful stop"
  echo " start       If we graceful start"
  echo " stop        we graceful stop"
  echo " debug       Print helpful debug information"
  exit 1
}

if [ $# -lt 1 ]; then
  usage
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`
# This will set HBASE_HOME, etc.
. "$bin"/hbase-config.sh
# Get arguments
start=
restart=
stop=
reload=
debug=
while [ $# -gt 0 ]
do
  case "$1" in
    --start)  start=true; shift;;
    --restart)  restart=true; shift;;
    --stop)  stop=true; shift;;
    --debug)    debug="--debug"; shift;;
    --) shift; break;;
    -*) usage ;;
    *)  break;;	# terminate while loop
  esac
done

# "$@" contains the rest. Must be at least the hostname left.
#if [ $# -lt 1 ]; then
#  usage
#fi

hostname=`hostname -s`.`dnsdomainname`
filename="/tmp/$hostname"
# Run the region mover script.
echo "Disabling balancer! (if required)"
HBASE_BALANCER_STATE=`echo 'balance_switch false' | "$bin"/hbase --config ${HBASE_CONF_DIR} shell | tail -3 | head -1`
echo "Previous balancer state was $HBASE_BALANCER_STATE"
hosts="/tmp/$(basename $0).$$.tmp"
echo $hostname >> $hosts
if [ "$stop" != "" ]; then
  echo "Unloading $hostname region(s)"
  HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} org.jruby.Main "$bin"/region_mover.rb --file=$filename $debug unload $hostname
  echo "Unloaded $hostname region(s)"
  # Stop the server. Have to put hostname into its own little file for hbase-daemons.sh
  "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} stop regionserver
fi
if [ "$start" != "" ]; then
  "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} start regionserver
  if [ -f $filename ]; then
    echo "Reloading $hostname region(s)"
    echo "${HBASE_CONF_DIR} org.jruby.Main $bin/region_mover.rb --file=$filename $debug load $hostname"
    HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} org.jruby.Main "$bin"/region_mover.rb --file=$filename $debug load $hostname
    echo "Reloaded $hostname region(s)"
  else
    echo "not have $filename"
  fi
fi

if [ "$restart" != "" ]; then
  echo "Unloading $hostname region(s)"
  HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} org.jruby.Main "$bin"/region_mover.rb --file=$filename $debug unload $hostname
  echo "Unloaded $hostname region(s)"
  # Stop the server. Have to put hostname into its own little file for hbase-daemons.sh
  "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} stop regionserver

  "$bin"/hbase-daemons.sh --config ${HBASE_CONF_DIR} --hosts ${hosts} start regionserver
  if [ -f $filename ]; then
    echo "Reloading $hostname region(s)"
    echo "${HBASE_CONF_DIR} org.jruby.Main $bin/region_mover.rb --file=$filename $debug load $hostname"
    HBASE_NOEXEC=true "$bin"/hbase --config ${HBASE_CONF_DIR} org.jruby.Main "$bin"/region_mover.rb --file=$filename $debug load $hostname
    echo "Reloaded $hostname region(s)"
  else
    echo "not have $filename"
  fi
fi


if [ $HBASE_BALANCER_STATE != "false" ]; then
  echo "Restoring balancer state to" $HBASE_BALANCER_STATE
  echo "balance_switch $HBASE_BALANCER_STATE" | "$bin"/hbase --config ${HBASE_CONF_DIR} shell &> /dev/null
fi

# Cleanup tmp files.
trap "rm -f  "/tmp/$(basename $0).*.tmp" &> /dev/null" EXIT
