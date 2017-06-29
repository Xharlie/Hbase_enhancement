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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Strings;

public class DefaultSedaStage implements Stage {
  protected static final Log LOG = LogFactory.getLog(DefaultSedaStage.class);

  protected final String name;
  protected int handlerCount;
  protected int queueSize;
  protected List<StageHandler> handlers;

  protected StageManager stageManager;
  protected Configuration conf = null;
  private RoundRobinStageDistributionStrategy strategy;

  public DefaultSedaStage(final String name) {
    this.name = Strings.nullToEmpty(name);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void init(Configuration conf) {
    this.conf = conf;
    // e.g.  stage.PreAppendStage.handler.count
    this.handlerCount = this.conf.getInt("stage." + name + ".handler.count", 20);
    this.queueSize = this.conf.getInt("stage." + name + ".queue.size", 2048);
    this.handlers = new ArrayList<StageHandler>(handlerCount);
    strategy = new RoundRobinStageDistributionStrategy(handlerCount);
    LOG.info("stage." + name + ".handler.count=" + handlerCount + ", " + "stage." + name
        + ".queue.size=" + queueSize);
  }

  @Override
  public void start() {
    for (int i = 0; i < handlerCount; i++) {
      BlockingQueue<Task> queue = new LinkedBlockingQueue<Task>(queueSize);
      StageHandler handler = new StageHandler(queue);
      handler.setRunning(true);
      handler.setDaemon(true);
      handler.setName("Stage=" + name + ",handler=" + i);
      handler.start();
      handlers.add(handler);
    }
  }

  @Override
  public void stop() {
    for (StageHandler handler : handlers) {
      handler.setRunning(false);
    }
    for (StageHandler handler : handlers) {
      handler.interrupt();
    }
  }

  @Override
  public void add(Task t) throws InterruptedException {
    t.setCurrentStage(this);
    // regionName hash to different queue.
//    int index = Math.abs(((AbstractTask) t).getRegionName().hashCode()) % handlers.size();
//    handlers.get(index).add(t);
    handlers.get(strategy.getIndex(((AbstractTask) t).getRegionName())).add(t);
  }

  @Override
  public StageManager getStageManager() {
    return stageManager;
  }

  @Override
  public void setStageManager(StageManager manager) {
    this.stageManager = manager;
  }

}
