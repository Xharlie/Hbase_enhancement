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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configuration;

public interface Stage {

  public static final String PreAppendStage = "PreAppendStage";
  public static final String FinishSyncStage = "FinishSyncStage";
  public static final String FinishAsyncStage = "FinishAsyncStage";
  public static final String MultiGetStage = "MultiGetStage";

  /**
   * get name of the stage
   * @return
   */
  public String getName();

  /**
   * initialize
   */
  public void init(Configuration conf);

  /**
   * start
   */
  public void start();

  /**
   * shutdown this stage
   */
  public void stop();

  public StageManager getStageManager();

  public void setStageManager(StageManager manager);

  public void add(Task t) throws InterruptedException;

}