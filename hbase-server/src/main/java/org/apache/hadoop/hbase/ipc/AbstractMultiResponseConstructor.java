package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.Message;

public abstract class AbstractMultiResponseConstructor {
  private static final Log LOG = LogFactory.getLog(AbstractMultiResponseConstructor.class);

  protected CallRunner callTask;
  protected MultiResponse.Builder responseBuilder;
  protected long startTime;
  protected CellScanner cellScanner = null;
  // pure gets or puts doesn't need errorThrowable
  protected Throwable errorThrowable = null;

  public AbstractMultiResponseConstructor(CallRunner callTask) {
    this.callTask = callTask;
    this.responseBuilder = MultiResponse.newBuilder();
    this.startTime = System.currentTimeMillis();
  }

  public void doResponse() {
    RpcCall call = callTask.getRpcCall();
    try {
      Message result = responseBuilder.build();
      ((RpcServer) this.callTask.getRpcServer()).updateCallMetrics(this.startTime, result,
        (RpcServer.Call) call);
      if (!call.isDelayed() || !call.isReturnValueDelayed()) {
        String err =
            (errorThrowable != null) ? StringUtils.stringifyException(errorThrowable) : null;
        call.setResponse(result, cellScanner, errorThrowable, err);
      }
      call.sendResponseIfReady();
    } catch (Exception e) {
      LOG.warn("multiRequest doResponse exception", e);
    } finally {
      this.callTask.getRpcServer().addCallSize(call.getSize() * -1);
      this.callTask.cleanup();
      this.cleanup();
    }
  }

  public abstract void cleanup();
}
