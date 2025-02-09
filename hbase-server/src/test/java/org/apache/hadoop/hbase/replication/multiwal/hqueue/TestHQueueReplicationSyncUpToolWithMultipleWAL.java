/**
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
package org.apache.hadoop.hbase.replication.multiwal.hqueue;

import static org.apache.hadoop.hbase.HQueueTestingConstants.HQUEUE_COPROCESSOR_KEY;
import static org.apache.hadoop.hbase.HQueueTestingConstants.HQUEUE_COPROCESSOR_VALUE;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.replication.multiwal.TestReplicationSyncUpToolWithMultipleWAL;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestHQueueReplicationSyncUpToolWithMultipleWAL extends
    TestReplicationSyncUpToolWithMultipleWAL {
  @Override
  protected void setTableDescriptor(HTableDescriptor tableDescriptor) {
    tableDescriptor.setValue(HQUEUE_COPROCESSOR_KEY, HQUEUE_COPROCESSOR_VALUE);
  }
}
