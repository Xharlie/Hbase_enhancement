/**
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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.HQueueTestingConstants.HQUEUE_COPROCESSOR_KEY;
import static org.apache.hadoop.hbase.HQueueTestingConstants.HQUEUE_COPROCESSOR_VALUE;
import static org.apache.hadoop.hbase.HQueueTestingConstants.COMPACTION_HFILE_NUM_THRESHOLD;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestHQueueLogRolling extends TestLogRolling {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestLogRolling.setUpBeforeClass();
    TEST_UTIL.getConfiguration().setInt(COMPACTION_HFILE_NUM_THRESHOLD, 3);
  }

  @Override
  protected void setHTableDescriptor(HTableDescriptor tableDescriptor) {
    tableDescriptor.setValue(HQUEUE_COPROCESSOR_KEY, HQUEUE_COPROCESSOR_VALUE);
  }
}
