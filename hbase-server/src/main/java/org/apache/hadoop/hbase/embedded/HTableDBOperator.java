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
package org.apache.hadoop.hbase.embedded;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HTableDBOperator extends DBOperator {

  private final Admin admin;

  public HTableDBOperator(EmbeddedHBase eHBase, Table table) throws IOException {
    super(eHBase, table);
    admin = eHBase.getConnection().getAdmin();
  }

  @Override
  public boolean exists(Get get) throws IOException {
    return table.exists(get);
  }

  @Override
  public boolean[] existsAll(List<Get> gets) throws IOException {
    return table.existsAll(gets);
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    table.batch(actions, results);
  }

  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
    return table.batch(actions);
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    table.batchCallback(actions, results, callback);
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback)
      throws IOException, InterruptedException {
    return table.batchCallback(actions, callback);
  }

  @Override
  public Result get(Get get) throws IOException {
    return table.get(get);
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    return table.get(gets);
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return table.getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return table.getScanner(family);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return table.getScanner(family, qualifier);
  }

  @Override
  public void put(Put put) throws IOException {
    table.put(put);
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    table.put(puts);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    return table.checkAndPut(row, family, qualifier, value, put);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, Put put) throws IOException {
    return table.checkAndPut(row, family, qualifier, compareOp, value, put);
  }

  @Override
  public void delete(Delete delete) throws IOException {
    table.delete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    table.delete(deletes);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws IOException {
    return table.checkAndDelete(row, family, qualifier, value, delete);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, Delete delete) throws IOException {
    return table.checkAndDelete(row, family, qualifier, compareOp, value, delete);
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    table.mutateRow(rm);
  }

  @Override
  public Result append(Append append) throws IOException {
    return table.append(append);
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    return table.increment(increment);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    return table.incrementColumnValue(row, family, qualifier, amount);
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability) throws IOException {
    return table.incrementColumnValue(row, family, qualifier, amount, durability);
  }

  @Override
  public void close() throws IOException {
    table.close();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return table.coprocessorService(row);
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable) throws ServiceException, Throwable {
    return table.coprocessorService(service, startKey, endKey, callable);
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable, Callback<R> callback) throws ServiceException, Throwable {
    table.coprocessorService(service, startKey, endKey, callable, callback);
  }

  @Override
  public long getWriteBufferSize() {
    return table.getWriteBufferSize();
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    table.setWriteBufferSize(writeBufferSize);
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey,
      R responsePrototype) throws ServiceException, Throwable {
    return table.batchCoprocessorService(methodDescriptor, request, startKey, endKey,
      responsePrototype);
  }

  @Override
  public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor,
      Message request, byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
      throws ServiceException, Throwable {
    table.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype, callback);
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, RowMutations mutation) throws IOException {
    return table.checkAndMutate(row, family, qualifier, compareOp, value, mutation);
  }

  @Override
  public void snapshot(String snapshotName) throws IOException {
    // do snapshot with Admin
    admin.snapshot(snapshotName, table.getName());
  }

  @Override
  public void snapshot(String snapshotName, HBaseProtos.SnapshotDescription.Type type) throws IOException {
    admin.snapshot(snapshotName, table.getName(), type);
  }

  @Override
  public void deleteSnapshot(String snapshotName) throws IOException {
    admin.deleteSnapshot(snapshotName);
  }

  @Override
  public void restoreSnapshot(String snapshotName, String tableName) throws IOException {
    admin.restoreSnapshot(snapshotName);
  }

}
