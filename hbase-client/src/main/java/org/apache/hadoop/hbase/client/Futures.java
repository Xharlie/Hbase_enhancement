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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Static utility methods pertaining to the {@link Future} interface.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Futures {

    /**
     * Registers separate success and failure listener to be run when the
     * {@code Future}'s computation is {@linkplain Future#isDone()
     * complete} or, if the computation is already complete, immediately.
     *
     * <p>The callback is run in {@code executor}. There is no guaranteed ordering
     * of execution of callbacks, but any callback added through this method is
     * guaranteed to be called once the computation is complete.
     *
     * @param future The future attach the listener to.
     * @param listener The listener(callback) to invoke when {@code future} is completed
     * @param executor The executor to run {@code listener} when the future completes.
     * @return
     */
    public static <S, F extends Future<S>> F addListener(F future, Listener<S> listener,
                                                         Executor executor) {
        if (future instanceof AsyncFuture){
            return (F) ((AsyncFuture) future).addListener(listener, executor);
        } else {
            //TODO: complete the logic here when migrating to hbase 2.0
            return future;
        }
    }
}
