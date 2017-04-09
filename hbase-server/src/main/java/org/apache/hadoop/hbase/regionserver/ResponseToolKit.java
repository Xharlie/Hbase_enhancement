package org.apache.hadoop.hbase.regionserver;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.ipc.Call;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
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
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by xharlie on 7/25/16.
 */
public class ResponseToolKit {

    protected static final Log LOG = LogFactory.getLog(ResponseToolKit.class);
    public HRegion hregion;
    public MultiResponse.Builder multiResponseBuilder;
    public ClientProtos.MutateResponse.Builder mutateResponseBuilder;
    public PayloadCarryingRpcController controller;
    public RegionActionResult.Builder regionActionResultBuilder;
    public List<CellScannable> cellsToReturn;
    public List<ClientProtos.Action> mutations;
    public Map<byte[], List<Cell>>[] familyMaps;
    public Durability durability;
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
    public HRegion.BatchOperationInProgress<?> batchOp;
    public Mutation mutation;
    public boolean responded;
    public Mutation[] mArray;

    public void setMultiResponseToolKit(Region region, MultiResponse.Builder responseBuilder,
                    RegionActionResult.Builder regionActionResultBuilder, List<CellScannable> cellsToReturn ){
        this.hregion = (HRegion)region;
        this.multiResponseBuilder = responseBuilder;
        this.regionActionResultBuilder = regionActionResultBuilder;
        this.cellsToReturn = cellsToReturn;
    }

    public void setMutateResponseToolKit(Region region, ClientProtos.MutateResponse.Builder responseBuilder){
        this.hregion = (HRegion)region;
        this.mutateResponseBuilder = responseBuilder;
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
    }

    public Throwable addMutateException(Throwable errorThrowable, OperationStatus[] retCodeDetails) {
        Throwable err = null;
        if (errorThrowable != null) {
            err = errorThrowable;
        } else {
            if (retCodeDetails[0].equals(HConstants.OperationStatusCode.SANITY_CHECK_FAILURE)) {
                err = new FailedSanityCheckException(retCodeDetails[0].getExceptionMsg());
            } else if (retCodeDetails[0].getOperationStatusCode().equals(HConstants.OperationStatusCode.BAD_FAMILY)) {
                err = new NoSuchColumnFamilyException(retCodeDetails[0].getExceptionMsg());
            }
        }
        if (err != null) err = new ServiceException(err);
        try {
            regionServer.checkFileSystem();
        } catch (Exception ie) {
            err = ie;
        }
        return err;
    }

    public void respond(OperationStatus[] retCodeDetails, Throwable errorThrowable, String error) throws IOException{
        responded = true;
        Message result;
        Throwable err = errorThrowable;
        String errString = error;
        if (multiResponseBuilder != null) {
            addResultToBuilder(retCodeDetails);
            result = multiResponseBuilder.build();
        } else if (mutateResponseBuilder != null) {
            err = addMutateException(errorThrowable, retCodeDetails);
            if (err == null) {
                addResult(mutateResponseBuilder, null, controller);
            } else {
                errString = err.getMessage();
            }
            mutateResponseBuilder.setProcessed(true);
            result = mutateResponseBuilder.build();
        } else {
            LOG.fatal("Both multiResponseBuilder and mutateResponseBuilder is null when responding");
            return;
        }
        RpcServer.updateCallMetrics(startTime, receiveTime, result, call, status, md, param);
        if (!call.isDelayed() || !call.isReturnValueDelayed()) {
            CellScanner cells = controller != null ? controller.cellScanner() : null;
            call.setResponse(result, cells, err, errString);
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

    private void addResult(final ClientProtos.MutateResponse.Builder builder, final Result result,
                           final PayloadCarryingRpcController rpcc) {
        if (result == null) return;
        if (call != null && call.isClientCellBlockSupport()) {
            builder.setResult(ProtobufUtil.toResultNoData(result));
            rpcc.setCellScanner(result.cellScanner());
        } else {
            ClientProtos.Result pbr = ProtobufUtil.toResult(result);
            builder.setResult(pbr);
        }
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
                            hregion.getRegionStats()));
                    break;
            }
        }
        if (regionServer.metricsRegionServer != null) {
            long after = EnvironmentEdgeManager.currentTime();
            if (batchContainsPuts) {
                regionServer.metricsRegionServer.updatePut(after - before);
            }
            if (batchContainsDelete) {
                regionServer.metricsRegionServer.updateDelete(after - before);
            }
        }
        multiResponseBuilder.addRegionActionResult(regionActionResultBuilder.build());
        // Load the controller with the Cells to return.
        if (cellsToReturn != null && !cellsToReturn.isEmpty() && controller != null) {
            controller.setCellScanner(CellUtil.createCellScanner(cellsToReturn));
        }
    }

    public HRegion getRegion() {
        return hregion;
    }

    public List<ClientProtos.Action> getMutations() {
        return mutations;
    }

    public void setMutations(List<ClientProtos.Action> mutations) {
        this.mutations = mutations;
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
