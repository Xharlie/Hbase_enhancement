#!/bin/bash

thrift -strict --gen java:hashcode -out hbase-thrift/src/main/java/ hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift

exit 0;
