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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;

/**
 * Used to communicate with a single HBase table, with asynchronous API supported
 * @since 1.1.2
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface AsyncableHTableInterface extends HTableInterface {
  /**
   * Extracts certain cells from a given row.
   * <p/>
   * It is an async operation, but won't retry. If auto-retry is needed, please use
   * {@link #asyncGet(Get)}
   * <p/>
   * You should define the action for result or error in given callback.
   * @param get The object that specifies what data to fetch and from which row.
   * @param callback define how to handle the remote response
   * @throws IOException
   */
  public void asyncGet(final Get get, AsyncRpcCallback<GetResponse> callback) throws IOException;

  /**
   * Extracts certain cells from a given row. It is an async operation and you could get the result
   * from the returned future
   * @param get The object that specifies what data to fetch and from which row
   * @param callback define how to handle the remote response
   * @return an {@link Future} to get the result
   * @throws IOException if an error occurs
   */
  public Future<Result> asyncGet(final Get get) throws IOException;

  /**
   * Put one row data into the table.
   * <p/>
   * It is an async operation, but won't retry. If auto-retry is needed, please use
   * {@link #asyncPut(Put)}
   * <p/>
   * You should define the action for result or error in given callback.
   * @param put The data to put.
   * @param callback define how to handle the remote response
   * @throws IOException if an error occurs
   */
  public void asyncPut(final Put put, AsyncRpcCallback<MutateResponse> callback) throws IOException;

  /**
   * Puts a row data in the table. It is an async operation and you could get the result from the
   * returned future
   * @param put The data to put.
   * @return an {@link Future} to get the result
   * @throws IOException if an error occurs
   */
  public Future<Result> asyncPut(Put put) throws IOException;

  /**
   * Deletes the specified cells/row.
   * <p/>
   * It is an async operation, but won't retry. If auto-retry is needed, please use
   * {@link #asyncDelete(Delete)}
   * <p/>
   * You should define the action for result or error in given callback.
   * @param delete List of things to delete.
   * @param callback define how to handle the remote response
   * @throws IOException
   */
  public void asyncDelete(final Delete delete, AsyncRpcCallback<MutateResponse> callback)
      throws IOException;

  /**
   * Deletes the specified cells/row. It is an async operation and you could get the result from the
   * returned future
   * @param delete The data to put.
   * @return an {@link Future} to get the result
   * @throws IOException if an error occurs
   */
  public Future<Result> asyncDelete(Delete delete) throws IOException;

  /**
   * Extracts certain cells from the given rows, in batch.
   * <p/>
   * It is an async operation, but won't retry. If auto-retry is needed, please use
   * @param gets
   * @param results the array to store results
   * @param batchCallback
   * @throws IOException
   */
  public void asyncBatchGet(List<Get> gets, Object[] results, AsyncBatchCallback batchCallback)
      throws IOException;

  /**
   * Extracts certain cells from the given rows, in batch.
   * <p/>
   * It is an async operation, but won't retry. If auto-retry is needed, please use
   * @param puts
   * @param results the array to store results
   * @param batchCallback
   * @throws IOException
   */
  public void asyncBatchPut(List<Put> puts, Object[] results, AsyncBatchCallback batchCallback)
      throws IOException;

  /**
   * Deletes the specified cells/rows in bulk.
   * <p/>
   * It is an async operation, but won't retry. If auto-retry is needed, please use
   * @param deletes
   * @param results the array to store results
   * @param batchCallback
   * @throws IOException
   */
  public void asyncBatchDelete(List<Delete> deletes, Object[] results,
      AsyncBatchCallback batchCallback) throws IOException;

  /**
   * Method that does a batch call on Deletes, Gets and Puts. The ordering of execution of the
   * actions is not defined. Meaning if you do a Put and a Get in the same
   * {@link #asyncBatch(List, Object[])} call, you will not necessarily be guaranteed that the Get
   * returns what the Put had put.
   * <p/>
   * It is an async operation, and notice that we DON'T support replica reads in async semantic
   * @param rows
   * @param results the array to store results
   * @param batchCallback
   * @throws IOException
   */
  public void asyncBatch(List<? extends Row> rows, Object[] results,
      AsyncBatchCallback batchCallback) throws IOException;

  /**
   * @deprecated Please use {@link #asyncBatch(List, Object[])} instead
   */
  public AsyncFuture<Object[]> asyncBatch(List<? extends Row> rows) throws IOException;

  /**
   * Method that does a batch call on Deletes, Gets and Puts. The ordering of execution of the
   * actions is not defined. Meaning if you do a Put and a Get in the same
   * {@link #asyncBatch(List, Object[])} call, you will not necessarily be guaranteed that the Get
   * returns what the Put had put.
   * <p/>
   * It is an async operation, and notice that we DON'T support replica reads in async semantic
   * @param rows
   * @param results the array to store results
   * @return an {@link AsyncFuture} to get the result
   * @throws IOException
   */
  public AsyncFuture<Object[]> asyncBatch(List<? extends Row> rows, Object[] results)
      throws IOException;
}
