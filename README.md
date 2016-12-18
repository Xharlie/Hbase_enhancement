# HBASE Private Branch

This project is a private branch derived from Apache Hbase 

## Apache HBASE 

Apache HBase [1] is an open-source, distributed, versioned, column-oriented
store modeled after Google' Bigtable: A Distributed Storage System for
Structured Data by Chang et al.[2]  Just as Bigtable leverages the distributed
data storage provided by the Google File System, HBase provides Bigtable-like 
capabilities on top of Apache Hadoop

<https://github.com/apache/hbase/>

## Major MileStone

* **New rpc layer **: 
  Use Netty to facilitate the rpc, take the advantage of the direct buffer and EventLoop thread mechanism.
* **Direct Health Check**: 
  With regard to original health check setting, the health check chore is fired up by the server itself, in order to detect the hanging machine which wouldn’t allow new threads to be initiated.
* **New Encoding **: 
  Put Binary Search inside the file to achieve acceleration of the read operation and improvement of space usage. 
* **Overall Level Balancer **: 
  Simple balancer can only gurantee table level balance or cluster level, we need to empower the balancer by enable balance both at table level and cluster level
* **put Stage Event Driven**: 
  Separate the handler of CPU bounding activities from I/O bounding activities

  
