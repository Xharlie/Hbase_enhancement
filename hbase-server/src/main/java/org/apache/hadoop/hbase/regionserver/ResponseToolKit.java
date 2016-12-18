package org.apache.hadoop.hbase.regionserver;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.ipc.Call;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * Created by xharlie on 7/25/16.
 */
public class ResponseToolKit {

    public Region region;
    public MultiResponse.Builder responseBuilder;
    public PayloadCarryingRpcController controller;
    public RegionActionResult.Builder regionActionResultBuilder;
    public List<CellScannable> cellsToReturn;
    public List<ClientProtos.Action> mutations;
    public Map<byte[], List<Cell>>[] familyMaps;
    public long addedSize = 0;
    public long txid;
    public Durability durability;
    public List<MultiVersionConsistencyControl.WriteEntry> writeEntryList;
    public WALEdit[] walEdits;
    public RpcServer.Call call;
    public long receiveTime;
    public long startTime;
    public RpcServer RpcServer;
    public MonitoredRPCHandler status;
    public Descriptors.MethodDescriptor md;
    public Message param;
    public HRegionServer regionServer;
    public boolean batchContainsPuts;
    public boolean batchContainsDelete;
    public long before;
    public Throwable errorThrowable = null;
    public String error = null;
//    public BatchOperationInProgress<?> batchOp;

    public void setResponseToolKit(Region region, MultiResponse.Builder responseBuilder,
                    RegionActionResult.Builder regionActionResultBuilder, List<CellScannable> cellsToReturn ){
        this.region = region;
        this.responseBuilder = responseBuilder;
        this.regionActionResultBuilder = regionActionResultBuilder;
        this.cellsToReturn = cellsToReturn;
    }

    public void setRegionServer(HRegionServer regionServer){
        this.regionServer = regionServer;
    }

    public ResponseToolKit(RpcServer.Call call, PayloadCarryingRpcController controller, long receiveTime,
                           long startTime, RpcServer RpcServer, MonitoredRPCHandler status, Descriptors.MethodDescriptor md, Message param) {
        this.controller = controller;
        this.call = call;
        this.receiveTime = receiveTime;
        this.startTime = startTime;
        this.RpcServer = RpcServer;
        this.status = status;
        this.md = md;
        this.param = param;
        durability = Durability.USE_DEFAULT;
        writeEntryList = new LinkedList<>();
    }

    public void respond(OperationStatus[] retCodeDetails, Throwable errorThrowable, String error) throws IOException{
        addResultToBuilder(retCodeDetails);
        if (regionServer.metricsRegionServer != null) {
            long after = EnvironmentEdgeManager.currentTime();
            if (batchContainsPuts) {
                regionServer.metricsRegionServer.updatePut(after - before);
            }
            if (batchContainsDelete) {
                regionServer.metricsRegionServer.updateDelete(after - before);
            }
        }
        responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
        // Load the controller with the Cells to return.
        if (cellsToReturn != null && !cellsToReturn.isEmpty() && controller != null) {
            controller.setCellScanner(CellUtil.createCellScanner(cellsToReturn));
        }
        Message result = responseBuilder.build();
        RpcServer.updateCallMetrics(startTime, receiveTime, result, call, status, md, param);
        if (!call.isDelayed() || !call.isReturnValueDelayed()) {
            CellScanner cells = controller != null ? controller.cellScanner() : null;
            call.setResponse(result, cells, errorThrowable, error);
        }
        this.call.sendResponseIfReady();
    }

    private static ClientProtos.ResultOrException getResultOrException(
            final ClientProtos.Result r, final int index, final ClientProtos.RegionLoadStats stats) {
        return getResultOrException(ResponseConverter.buildActionResult(r, stats), index);
    }

    private static ClientProtos.ResultOrException getResultOrException(final Exception e, final int index) {
        return getResultOrException(ResponseConverter.buildActionResult(e), index);
    }

    private static ClientProtos.ResultOrException getResultOrException(
            final ClientProtos.ResultOrException.Builder builder, final int index) {
        return builder.setIndex(index).build();
    }

    public void addResultToBuilder(OperationStatus[] codes){
        for (int i = 0; i < codes.length; i++) {
            int index = mutations.get(i).getIndex();
            Exception e = null;
            switch (codes[i].getOperationStatusCode()) {
                case BAD_FAMILY:
                    e = new NoSuchColumnFamilyException(codes[i].getExceptionMsg());
                    regionActionResultBuilder.addResultOrException(getResultOrException(e, index));
                    break;

                case SANITY_CHECK_FAILURE:
                    e = new FailedSanityCheckException(codes[i].getExceptionMsg());
                    regionActionResultBuilder.addResultOrException(getResultOrException(e, index));
                    break;

                default:
                    e = new DoNotRetryIOException(codes[i].getExceptionMsg());
                    regionActionResultBuilder.addResultOrException(getResultOrException(e, index));
                    break;

                case SUCCESS:
                    regionActionResultBuilder.addResultOrException(getResultOrException(
                            ClientProtos.Result.getDefaultInstance(), index,
                            ((HRegion)region).getRegionStats()));
                    break;
            }
        }
    }

    public Region getRegion() {
        return region;
    }

    public void setRegion(Region region) {
        this.region = region;
    }

    public List<ClientProtos.Action> getMutations() {
        return mutations;
    }

    public void setMutations(List<ClientProtos.Action> mutations) {
        this.mutations = mutations;
    }

    public MultiResponse.Builder getResponseBuilder() {
        return responseBuilder;
    }

    public void setResponseBuilder(MultiResponse.Builder responseBuilder) {
        this.responseBuilder = responseBuilder;
    }

    public PayloadCarryingRpcController getController() {
        return controller;
    }

    public void setController(PayloadCarryingRpcController controller) {
        this.controller = controller;
    }

    public RegionActionResult.Builder getRegionActionResultBuilder() {
        return regionActionResultBuilder;
    }

    public void setRegionActionResultBuilder(RegionActionResult.Builder regionActionResultBuilder) {
        this.regionActionResultBuilder = regionActionResultBuilder;
    }

    public List<CellScannable> getCellsToReturn() {
        return cellsToReturn;
    }

    public void setCellsToReturn(List<CellScannable> cellsToReturn) {
        this.cellsToReturn = cellsToReturn;
    }
}
