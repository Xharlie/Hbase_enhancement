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

import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.HasThread;

public class StageHandler extends HasThread {
  private static final Log LOG = LogFactory.getLog(StageHandler.class);

  private boolean running;
  private final BlockingQueue<Task> queue;

  public StageHandler(BlockingQueue<Task> queue) {
    this.queue = queue;
  }

  public void setRunning(boolean running) {
    this.running = running;
  }

  public void add(Task task) throws InterruptedException {
     queue.put(task);
  }

  @Override
  public void run() {
    boolean interrupted = false;
    try {
      while (running) {
        try {
          Task task = queue.take();
          try {
            task.run();
          } catch (Throwable e) {
            LOG.warn("task run error.", e);
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

}
