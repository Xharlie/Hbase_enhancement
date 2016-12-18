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
package org.apache.hadoop.hbase;

/**
 * Holds a bunch of constants for HQueue related testing
 */
public final class HQueueTestingConstants {
  public static final String HQUEUE_COPROCESSOR_KEY = "coprocessor$1";
  public static final String HQUEUE_COPROCESSOR_VALUE =
      "|com.etao.hadoop.hbase.queue.coprocessor.HQueueCoprocessor|1073741823|";
  public static String COMPACTION_HFILE_NUM_THRESHOLD = "hqueue.compaction.hfileNumThreshold";
}
