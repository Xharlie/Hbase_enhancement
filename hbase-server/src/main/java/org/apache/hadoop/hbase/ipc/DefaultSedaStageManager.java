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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;

public class DefaultSedaStageManager implements StageManager {
  private Map<String, Stage> stages = new ConcurrentHashMap<String, Stage>();
  private boolean started;
  private Configuration conf;

  public DefaultSedaStageManager() {
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void register(Stage stage) {
    stages.put(stage.getName(), stage);
    stage.setStageManager(this);
    stage.init(conf);
  }

  @Override
  public Stage getStage(String name) {
    if (stages.containsKey(name)) {
      return stages.get(name);
    } else {
      throw new IllegalStateException("No such stage: " + name);
    }
  }

  @Override
  public List<Stage> getStages() {
    return new ArrayList<Stage>(stages.values());
  }

  @Override
  public synchronized void start() {
    if (started) {
      return;
    }
    for (Stage stage : stages.values()) {
      stage.start();
    }
    started = true;
  }

  @Override
  public synchronized void shutdown() {
    if (!started) {
      return;
    }
    for (Stage stage : stages.values()) {
      stage.stop();
    }
    stages.clear();
    started = false;
  }

}
