/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

/**
 * Tracker on cluster settings up in zookeeper. This class is about tracking
 * cluster availability in zookeeper.
 *
 */
@InterfaceAudience.Private
public class ClusterAvailabilityTracker extends ZooKeeperNodeTracker {
    private static final Log LOG = LogFactory.getLog(ClusterAvailabilityTracker.class);

    /**
     * Creates a cluster availability tracker.
     * <p>
     * After construction, use {@link #start} to kick off tracking.
     * @param watcher
     * @param abortable
     */
    public ClusterAvailabilityTracker(ZooKeeperWatcher watcher, Abortable abortable) {
        super(watcher, watcher.clusterAvailabilityZNode, abortable);
    }

    /**
     * Checks if cluster is available.
     * @return true if the cluster is available, false if not
     */
    public boolean isClusterAvailable() {
        return super.getData(false) == null;
    }

    /**
     * Sets the cluster as available.
     * @throws KeeperException unexpected zk exception
     */
    public void setClusterAvailable()
            throws KeeperException {
        try {
            ZKUtil.deleteNode(watcher, watcher.clusterAvailabilityZNode);
        } catch (KeeperException.NoNodeException nne) {
            LOG.warn("Attempted to set cluster as available but the cluster availability node ("
                    + watcher.clusterAvailabilityZNode + ") was not found!");
        }
    }

    /**
     * Sets the cluster as unavailable by creating the znode.
     * @throws KeeperException unexpected zk exception
     */
    public void setClusterUnavailable()
            throws KeeperException {
        byte[] upData = Bytes.toBytes(String.valueOf(System.currentTimeMillis()));
        try {
            ZKUtil.createAndWatch(watcher, watcher.clusterAvailabilityZNode, upData);
        } catch (KeeperException.NodeExistsException nee) {
            ZKUtil.setData(watcher, watcher.clusterAvailabilityZNode, upData);
        }
    }
}

