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

package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.regionserver.RSRpcServices.RegionScannersCloseCallBack;

public class MultiGetResponseConstructor extends AbstractMultiResponseConstructor {
  private static final Log LOG = LogFactory.getLog(MultiGetResponseConstructor.class);

  private int getActionCount;
  private int getFinished = 0;
  private CloseCallBack callback = new CloseCallBack();

  private List<List<ResultOrException>> results = null;
  private List<List<CellScannable>> cells = null;

  public MultiGetResponseConstructor(CallRunner callTask, int actionCount, int regionCount) {
    super(callTask);
    this.getActionCount = actionCount;
    this.results = new ArrayList<>(regionCount);
    this.cells = new ArrayList<>(regionCount);
  }

  public void allocate(int regionIndex, int actionNum) {
    List<ResultOrException> subResults = new ArrayList<ResultOrException>(actionNum);
    List<CellScannable> subCells = new ArrayList<CellScannable>(actionNum);
    for (int i = 0; i < actionNum; i++) {
      subResults.add(null);
      subCells.add(null);
    }
    results.add(subResults);
    cells.add(subCells);
  }

  public synchronized void addResult(int regionIndex, int actionIndex,
      ResultOrException resultOrException, CellScannable cellsToReturn,
      RegionScannersCloseCallBack closeCallBack) throws IOException {
    getFinished++;
    List<ResultOrException> subResults = results.get(regionIndex);
    subResults.set(actionIndex, resultOrException);
    List<CellScannable> subCells = cells.get(regionIndex);
    subCells.set(actionIndex, cellsToReturn);
    callback.addCallback(closeCallBack);
    if (getFinished == getActionCount) {
      RegionActionResult.Builder regionActionResultBuilder = RegionActionResult.newBuilder();
      List<CellScannable> totalCellsToReturn = null;
      for (int i = 0; i < results.size(); i++) {
        regionActionResultBuilder.clear();
        List<ResultOrException> subs = results.get(i);
        for (ResultOrException sub : subs) {
          regionActionResultBuilder.addResultOrException(sub);
        }
        List<CellScannable> regionCells = cells.get(i);
        for (CellScannable regionCell : regionCells) {
          if (regionCell != null) {
            if (totalCellsToReturn == null) {
              // Hard to guess the size here. Just make a rough guess.
              totalCellsToReturn = new ArrayList<CellScannable>();
            }
            totalCellsToReturn.add(regionCell);
          }
        }
        responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
      }
      if (totalCellsToReturn != null && !totalCellsToReturn.isEmpty()) {
        cellScanner = CellUtil.createCellScanner(totalCellsToReturn);
      }
      callTask.getRpcCall().setCallBack(callback);
      doResponse();
    }
  }

  @Override
  public void cleanup() {
    this.callTask = null;
    this.cellScanner = null;
    this.responseBuilder = null;
  }

}
