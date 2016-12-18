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
package org.apache.hadoop.hbase.client;

import java.util.List;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Wrapper class for the real callback, used for recording the MultiAction per server.
 * <p/>
 * We will group the batch request to server-region-actions mappings, and use this mapping to deal
 * with the response. Since user-customized callback won't know such mapping, we need this wrapper
 * to store the mapping and delegate real handling to user callback
 * <p/>
 * Notice that we will also save such mappings in SingleServerRequestRunnable inside
 * {@link AsyncProcess}, so it's not a new problem if considering callback map size
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncBatchCallbackWrapper extends AsyncBatchCallback {
  AsyncBatchCallback delegate;

  public AsyncBatchCallbackWrapper(MultiAction<Row> multiAction, ServerName server,
      AsyncBatchCallback callback) {
    super(callback.results, callback.getActionsInProgress(), callback.getActions(), multiAction,
        server);
    this.delegate = callback;
  }

  @Override
  public void processSingleResult(MultiAction<Row> multiAction, MultiResponse result,
      ServerName server) {
    this.delegate.processSingleResult(multiAction, result, server);
  }

  @Override
  public void processBatchResult(Object[] results) {
    this.delegate.processBatchResult(results);
  }

  @Override
  public void processError(Throwable exception, List<Action<Row>> toRetry) {
    this.delegate.processError(exception, toRetry);
  }

}
