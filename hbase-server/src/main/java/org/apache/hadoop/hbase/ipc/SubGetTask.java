package org.apache.hadoop.hbase.ipc;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RSRpcServices.RegionScannersCloseCallBack;

public class SubGetTask extends AbstractTask {
  private static final Log LOG = LogFactory.getLog(SubGetTask.class);

  private final CallRunner callRunner;
  private final MultiGetResponseConstructor responseBuilder;
  private final RegionAction regionAction;
  private final ClientProtos.Action action;
  private final int regionIndex;
  private final int actionIndex;

  public SubGetTask(CallRunner callRunner, MultiGetResponseConstructor responseBuilder,
      RegionAction regionAction, ClientProtos.Action action, int regionIndex, int actionIndex) {
    this.callRunner = callRunner;
    this.responseBuilder = responseBuilder;
    this.regionAction = regionAction;
    this.action = action;
    this.regionIndex = regionIndex;
    this.actionIndex = actionIndex;
  }

  @Override
  public void run() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Run subtask: " + action);
    }
    ClientProtos.ResultOrException.Builder resultOrExceptionBuilder =
        ClientProtos.ResultOrException.newBuilder();
    resultOrExceptionBuilder.setIndex(action.getIndex());
    RegionScannersCloseCallBack closeCallBack = new RegionScannersCloseCallBack();
    CellScannable cellsToReturn = null;
    try {
      if (action.hasGet()) {
        Get get = ProtobufUtil.toGet(action.getGet());
        RSRpcServices rs = callRunner.getRpcServer().getRsRpcServices();
        Result r = rs.get(regionAction.getRegion(), get, closeCallBack, null);
        if (r != null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Run subtask result is: " + r);
          }
          ClientProtos.Result pbResult = null;
          if (callRunner.getRpcCall().isClientCellBlockSupport()) {
            pbResult = ProtobufUtil.toResultNoData(r);
            cellsToReturn = r;
          } else {
            pbResult = ProtobufUtil.toResult(r);
          }
          resultOrExceptionBuilder.setResult(pbResult);
        }
      } else {
        throw new IOException("Unexpected action, get action expected.");
      }
    } catch (Exception e) {
      resultOrExceptionBuilder.setException(ResponseConverter.buildException(e));
    }
    try {
      responseBuilder.addResult(regionIndex, actionIndex, resultOrExceptionBuilder.build(),
        cellsToReturn, closeCallBack);
    } catch (Exception e) {
      LOG.debug("Throw unexpected error", e);
    }
  }

  @Override
  public String getRegionName() {
    return regionAction.getRegion().getValue().toStringUtf8();
  }

}
