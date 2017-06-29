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
package org.apache.hadoop.hbase.client;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.*;


/**
 * A short-circuit connection that can bypass the RPC layer (serialization, deserialization,
 * networking, etc..) when talking to a local master
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ShortCircuitMasterKeepAliveConnection implements MasterKeepAliveConnection {

  private final MasterService.BlockingInterface stub;

  public ShortCircuitMasterKeepAliveConnection(MasterService.BlockingInterface stub) {
    this.stub = stub;
  }

  @Override
  public UnassignRegionResponse unassignRegion(RpcController controller,
      UnassignRegionRequest request) throws ServiceException {
    return stub.unassignRegion(controller, request);
  }

  @Override
  public OfflineRegionResponse offlineRegion(RpcController controller, OfflineRegionRequest request) throws ServiceException {
      return stub.offlineRegion(controller, request);
  }

    @Override
  public TruncateTableResponse truncateTable(RpcController controller, TruncateTableRequest request)
      throws ServiceException {
    return stub.truncateTable(controller, request);
  }

  @Override
  public StopMasterResponse stopMaster(RpcController controller, StopMasterRequest request)
      throws ServiceException {
    return stub.stopMaster(controller, request);
  }

  @Override
  public SnapshotResponse snapshot(RpcController controller, SnapshotRequest request)
      throws ServiceException {
    return stub.snapshot(controller, request);
  }

  @Override
  public ShutdownResponse shutdown(RpcController controller, ShutdownRequest request)
      throws ServiceException {
    return stub.shutdown(controller, request);
  }

  @Override
  public SetQuotaResponse setQuota(RpcController controller, SetQuotaRequest request)
      throws ServiceException {
    return stub.setQuota(controller, request);
  }

  @Override
  public SetBalancerRunningResponse setBalancerRunning(RpcController controller,
      SetBalancerRunningRequest request) throws ServiceException {
    return stub.setBalancerRunning(controller, request);
  }

  @Override
  public RunCatalogScanResponse runCatalogScan(RpcController controller,
      RunCatalogScanRequest request) throws ServiceException {
    return stub.runCatalogScan(controller, request);
  }

  @Override
  public RestoreSnapshotResponse restoreSnapshot(RpcController controller,
      RestoreSnapshotRequest request) throws ServiceException {
    return stub.restoreSnapshot(controller, request);
  }

  @Override
  public IsRestoreSnapshotDoneResponse isRestoreSnapshotDone(RpcController controller, IsRestoreSnapshotDoneRequest request) throws ServiceException {
      return stub.isRestoreSnapshotDone(controller, request);
  }

    @Override
  public MoveRegionResponse moveRegion(RpcController controller, MoveRegionRequest request)
      throws ServiceException {
    return stub.moveRegion(controller, request);
  }

  @Override
  public ModifyTableResponse modifyTable(RpcController controller, ModifyTableRequest request)
      throws ServiceException {
    return stub.modifyTable(controller, request);
  }

  @Override
  public ModifyNamespaceResponse modifyNamespace(RpcController controller,
      ModifyNamespaceRequest request) throws ServiceException {
    return stub.modifyNamespace(controller, request);
  }

  @Override
  public ModifyColumnResponse modifyColumn(RpcController controller, ModifyColumnRequest request)
      throws ServiceException {
    return stub.modifyColumn(controller, request);
  }

  @Override
  public ListTableNamesByNamespaceResponse listTableNamesByNamespace(RpcController controller,
      ListTableNamesByNamespaceRequest request) throws ServiceException {
    return stub.listTableNamesByNamespace(controller, request);
  }

  @Override
  public ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(
      RpcController controller, ListTableDescriptorsByNamespaceRequest request)
      throws ServiceException {
    return stub.listTableDescriptorsByNamespace(controller, request);
  }

  @Override
  public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController controller,
      ListNamespaceDescriptorsRequest request) throws ServiceException {
    return stub.listNamespaceDescriptors(controller, request);
  }

  @Override
  public IsSnapshotDoneResponse isSnapshotDone(RpcController controller,
      IsSnapshotDoneRequest request) throws ServiceException {
    return stub.isSnapshotDone(controller, request);
  }

  @Override
  public IsProcedureDoneResponse isProcedureDone(RpcController controller,
      IsProcedureDoneRequest request) throws ServiceException {
    return stub.isProcedureDone(controller, request);
  }

  @Override
  public IsMasterRunningResponse isMasterRunning(RpcController controller,
      IsMasterRunningRequest request) throws ServiceException {
    return stub.isMasterRunning(controller, request);
  }

  @Override
  public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController controller,
      IsCatalogJanitorEnabledRequest request) throws ServiceException {
    return stub.isCatalogJanitorEnabled(controller, request);
  }

  @Override
  public ClientProtos.CoprocessorServiceResponse execMasterService(RpcController controller, ClientProtos.CoprocessorServiceRequest request) throws ServiceException {
      return stub.execMasterService(controller, request);
  }

    @Override
  public IsBalancerEnabledResponse isBalancerEnabled(RpcController controller,
      IsBalancerEnabledRequest request) throws ServiceException {
    return stub.isBalancerEnabled(controller, request);
  }

  @Override
  public GetTableNamesResponse getTableNames(RpcController controller, GetTableNamesRequest request)
      throws ServiceException {
    return stub.getTableNames(controller, request);
  }

  @Override
  public GetTableDescriptorsResponse getTableDescriptors(RpcController controller,
      GetTableDescriptorsRequest request) throws ServiceException {
    return stub.getTableDescriptors(controller, request);
  }

  @Override
  public GetSchemaAlterStatusResponse getSchemaAlterStatus(RpcController controller,
      GetSchemaAlterStatusRequest request) throws ServiceException {
    return stub.getSchemaAlterStatus(controller, request);
  }

  @Override
  public GetProcedureResultResponse getProcedureResult(RpcController controller,
      GetProcedureResultRequest request) throws ServiceException {
    return stub.getProcedureResult(controller, request);
  }

  @Override
  public GetNamespaceDescriptorResponse getNamespaceDescriptor(RpcController controller,
      GetNamespaceDescriptorRequest request) throws ServiceException {
    return stub.getNamespaceDescriptor(controller, request);
  }

  @Override
  public MajorCompactionTimestampResponse getLastMajorCompactionTimestampForRegion(
      RpcController controller, MajorCompactionTimestampForRegionRequest request)
      throws ServiceException {
    return stub.getLastMajorCompactionTimestampForRegion(controller, request);
  }

  @Override
  public MajorCompactionTimestampResponse getLastMajorCompactionTimestamp(RpcController controller,
      MajorCompactionTimestampRequest request) throws ServiceException {
    return stub.getLastMajorCompactionTimestamp(controller, request);
  }

  @Override
  public GetCompletedSnapshotsResponse getCompletedSnapshots(RpcController controller,
      GetCompletedSnapshotsRequest request) throws ServiceException {
    return stub.getCompletedSnapshots(controller, request);
  }

  @Override
  public GetClusterStatusResponse getClusterStatus(RpcController controller,
      GetClusterStatusRequest request) throws ServiceException {
    return stub.getClusterStatus(controller, request);
  }

  @Override
  public ExecProcedureResponse execProcedureWithRet(RpcController controller,
      ExecProcedureRequest request) throws ServiceException {
    return stub.execProcedureWithRet(controller, request);
  }

  @Override
  public ExecProcedureResponse execProcedure(RpcController controller, ExecProcedureRequest request)
      throws ServiceException {
    return stub.execProcedure(controller, request);
  }

  @Override
  public EnableTableResponse enableTable(RpcController controller, EnableTableRequest request)
      throws ServiceException {
    return stub.enableTable(controller, request);
  }

  @Override
  public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController controller,
      EnableCatalogJanitorRequest request) throws ServiceException {
    return stub.enableCatalogJanitor(controller, request);
  }

  @Override
  public DispatchMergingRegionsResponse dispatchMergingRegions(RpcController controller,
      DispatchMergingRegionsRequest request) throws ServiceException {
    return stub.dispatchMergingRegions(controller, request);
  }

  @Override
  public DisableTableResponse disableTable(RpcController controller, DisableTableRequest request)
      throws ServiceException {
    return stub.disableTable(controller, request);
  }

  @Override
  public DeleteTableResponse deleteTable(RpcController controller, DeleteTableRequest request)
      throws ServiceException {
    return stub.deleteTable(controller, request);
  }

  @Override
  public DeleteSnapshotResponse deleteSnapshot(RpcController controller,
      DeleteSnapshotRequest request) throws ServiceException {
    return stub.deleteSnapshot(controller, request);
  }

  @Override
  public DeleteNamespaceResponse deleteNamespace(RpcController controller,
      DeleteNamespaceRequest request) throws ServiceException {
    return stub.deleteNamespace(controller, request);
  }

  @Override
  public DeleteColumnResponse deleteColumn(RpcController controller, DeleteColumnRequest request)
      throws ServiceException {
    return stub.deleteColumn(controller, request);
  }

  @Override
  public CreateTableResponse createTable(RpcController controller, CreateTableRequest request)
      throws ServiceException {
    return stub.createTable(controller, request);
  }

  @Override
  public CreateNamespaceResponse createNamespace(RpcController controller,
      CreateNamespaceRequest request) throws ServiceException {
    return stub.createNamespace(controller, request);
  }

  @Override
  public BalanceResponse balance(RpcController controller, BalanceRequest request)
      throws ServiceException {
    return stub.balance(controller, request);
  }

  @Override
  public AssignRegionResponse assignRegion(RpcController controller, AssignRegionRequest request)
      throws ServiceException {
    return stub.assignRegion(controller, request);
  }

  @Override
  public AddColumnResponse addColumn(RpcController controller, AddColumnRequest request)
      throws ServiceException {
    return stub.addColumn(controller, request);
  }

  @Override
  public void close() {
    // nothing to do here
  }
}
