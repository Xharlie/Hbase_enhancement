package org.apache.hadoop.hbase.embedded;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RetriesExhaustedException.ThrowableWithExtraContext;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DirectDBOperator extends DBOperator {
  private final Log LOG = LogFactory.getLog(DirectDBOperator.class);
  final EmbeddedRpcServices rpcServices;
  final TableName tableName;
  final int maxRetries;
  final long pause;

  public DirectDBOperator(EmbeddedHBase eHBase, Table table) {
    super(eHBase, table);
    this.rpcServices = (EmbeddedRpcServices) eHBase.getRSRpcServices();
    this.tableName = table.getName();
    this.maxRetries = eHBase.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.pause = eHBase.getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
  }

  @Override
  public boolean exists(Get get) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean[] existsAll(List<Get> gets) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub

  }

  @Override public Object[] batch(List<? extends Row> actions)
      throws IOException, InterruptedException {
    return new Object[0];
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub

  }

  @Override public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback)
      throws IOException, InterruptedException {
    return new Object[0];
  }

  @Override
  public Result get(Get get) throws IOException {
    int tries = 0;
    List<ThrowableWithExtraContext> exceptions = null;
    while (true) {
      try {
        return rpcServices.get(tableName, get);
      } catch (IOException e) {
        checkRetry(tries, e, exceptions);
        Threads.sleep(ConnectionUtils.getPauseTime(pause, tries));
        tries++;
      }
    }
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    int tries = 0;
    List<ThrowableWithExtraContext> exceptions = null;
    while (true) {
      try {
        return rpcServices.getScanner(tableName, scan);
      } catch (IOException e) {
        checkRetry(tries, e, exceptions);
        Threads.sleep(ConnectionUtils.getPauseTime(pause, tries));
        tries++;
      }
    }
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void put(Put put) throws IOException {
    int tries = 0;
    List<ThrowableWithExtraContext> exceptions = null;
    while (true) {
      try {
        rpcServices.put(tableName, put);
        break;
      } catch (IOException e) {
        checkRetry(tries, e, exceptions);
        Threads.sleep(ConnectionUtils.getPauseTime(pause, tries));
        tries++;
      }
    }
  }

  private void checkRetry(int tries, IOException e, List<ThrowableWithExtraContext> exceptions)
      throws IOException {
    if (exceptions == null) {
      exceptions = new ArrayList<ThrowableWithExtraContext>();
    }
    exceptions.add(new ThrowableWithExtraContext(e, System.currentTimeMillis(), "tries=" + tries));
    if (tries >= maxRetries) {
      throw new RetriesExhaustedException(tries, exceptions);
    } else {
      LOG.debug("Caught exception, tries=" + tries, e);
    }
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, Put put) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void delete(Delete delete) throws IOException {
    int tries = 0;
    List<ThrowableWithExtraContext> exceptions = null;
    while (true) {
      try {
        rpcServices.delete(tableName, delete);
        break;
      } catch (IOException e) {
        checkRetry(tries, e, exceptions);
        Threads.sleep(ConnectionUtils.getPauseTime(pause, tries));
        tries++;
      }
    }
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
      Delete delete) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, Delete delete) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public Result append(Append append) throws IOException {
    int tries = 0;
    List<ThrowableWithExtraContext> exceptions = null;
    while (true) {
      try {
        return rpcServices.append(tableName, append);
      } catch (IOException e) {
        checkRetry(tries, e, exceptions);
        Threads.sleep(ConnectionUtils.getPauseTime(pause, tries));
        tries++;
      }
    }
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    int tries = 0;
    List<ThrowableWithExtraContext> exceptions = null;
    while (true) {
      try {
        return rpcServices.increment(tableName, increment);
      } catch (IOException e) {
        checkRetry(tries, e, exceptions);
        Threads.sleep(ConnectionUtils.getPauseTime(pause, tries));
        tries++;
      }
    }
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable) throws ServiceException, Throwable {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable, Callback<R> callback) throws ServiceException, Throwable {
    // TODO Auto-generated method stub

  }

  @Override
  public long getWriteBufferSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey,
      R responsePrototype) throws ServiceException, Throwable {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor,
      Message request, byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
      throws ServiceException, Throwable {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, RowMutations mutation) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void snapshot(String snapshotName) throws IOException {
    rpcServices.snapshot(snapshotName, tableName);
  }

  @Override public void snapshot(String snapshotName, HBaseProtos.SnapshotDescription.Type type)
      throws IOException {
    rpcServices.snapshot(snapshotName, tableName);
  }

  @Override
  public void deleteSnapshot(String snapshotName) throws IOException {
    rpcServices.deleteSnapshot(snapshotName);
  }

  @Override
  public void restoreSnapshot(String snapshotName, String tableName) throws IOException {
    rpcServices.restoreSnapshot(snapshotName, tableName);
  }

}
