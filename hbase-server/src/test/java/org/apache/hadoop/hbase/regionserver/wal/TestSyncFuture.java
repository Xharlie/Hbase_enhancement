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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSyncFuture {
  
  @Test(timeout = 60000)
  public void testGet() throws Exception {
    long timeout = 5000;
    long sequence = 100000;
    SyncFuture syncFulture = new SyncFuture();
    syncFulture.reset(sequence);
    syncFulture.done(sequence, null);
    assertEquals(sequence, syncFulture.get(timeout));

    syncFulture.reset(sequence);
    try {
      syncFulture.get(timeout);
      fail("Should have timed out but not");
    } catch (TimeoutIOException e) {
      // test passed
    }
  }

}
