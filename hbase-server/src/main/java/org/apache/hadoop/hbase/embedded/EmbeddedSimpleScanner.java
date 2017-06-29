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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EmbeddedSimpleScanner extends AbstractClientScanner {
    private static final Log LOG = LogFactory.getLog(EmbeddedSimpleScanner.class);

    protected Configuration conf;
    protected Scan scan;
    protected final TableName tableName;
    protected HRegion currentRegion;
    protected RegionScanner regionScanner;
    protected HRegionServer regionServer;

    protected boolean hasMoreResults = true;

    protected volatile boolean closed = false;

    public EmbeddedSimpleScanner(Configuration conf, Scan scan, TableName tableName, HRegionServer regionServer)
            throws IOException {
        this.conf = conf;
        this.scan = scan;
        this.tableName = tableName;
        this.regionServer = regionServer;
        initializeScannerInConstruction();
    }

    protected void initializeScannerInConstruction() throws IOException {
        nextScanner();
    }

    /*
   * Gets a scanner for the next region. If this.currentRegion != null, then we will move to the
   * endrow of this.currentRegion. Else we will get scanner at the scan.getStartRow(). We will go no
   * further, just tidy up outstanding scanners, if <code>currentRegion != null</code> and
   * <code>done</code> is true.
   */
    protected boolean nextScanner() throws IOException {
        // Where to start the next scanner
        byte[] localStartKey;
        if (this.currentRegion != null) {
            byte[] endKey = this.currentRegion.getRegionInfo().getEndKey();
            if (endKey == null || Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY)
                    || checkScanStopRow(endKey)) {
                close();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Finished " + this.currentRegion);
                }
                return false;
            }
            localStartKey = endKey;
            if (LOG.isTraceEnabled()) {
                LOG.trace("Finished " + this.currentRegion);
            }
        } else {
            localStartKey = this.scan.getStartRow();
        }

        if (LOG.isDebugEnabled() && this.currentRegion != null) {
            // Only worth logging if NOT first region in scan.
            LOG.trace(
                    "Advancing internal scanner to startKey at '" + Bytes
                        .toStringBinary(localStartKey) + "'");
        }

        try {
            this.currentRegion = (HRegion) getRegion(tableName, localStartKey);
            scan.setStartRow(localStartKey);
            this.regionScanner = this.currentRegion.getScanner(scan);
        } catch (IOException e) {
            close();
            throw e;
        }
        return true;
    }

    @Override
    public Result next() throws IOException {
        if (closed) {
            return null;
        }

        Result result = null;

        while(hasMoreResults || nextScanner()) {
            List<Cell> values = new ArrayList<Cell>(32);

            ScannerContext.Builder contextBuilder = ScannerContext.newBuilder(true);
            ScannerContext scannerContext = contextBuilder.build();

            // Collect values to be returned here
            currentRegion.startRegionOperation(Region.Operation.SCAN);
            try {
                hasMoreResults = regionScanner.nextRaw(values, scannerContext);
                if (!values.isEmpty()) {
                    result = Result.create(values, null, false, false);
                    break;
                }
            } finally {
                currentRegion.closeRegionOperation(Region.Operation.SCAN);
            }
        }

        return result;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        try {
            if (regionScanner != null) {
                regionScanner.close();
                regionScanner = null;
            }
        } catch (IOException e) {
            LOG.warn("scanner failed to close.", e);
        }
        closed = true;
    }

    @Override
    public boolean renewLease() {
        return false;
    }

    protected boolean checkScanStopRow(final byte[] endKey) {
        if (this.scan.getStopRow().length > 0) {
            // there is a stop row, check to see if we are past it.
            byte[] stopRow = scan.getStopRow();
            int cmp = Bytes.compareTo(stopRow, 0, stopRow.length, endKey, 0, endKey.length);
            if (scan.isReversed()) {
                if (cmp >= 0) {
                    // stopRow >= startKey (stopRow is equals to or larger than endKey)
                    // This is a stop.
                    return true;
                }
            } else {
                if (cmp <= 0) {
                    // stopRow <= endKey (endKey is equals to or larger than stopRow)
                    // This is a stop.
                    return true;
                }
            }
        }
        return false; // unlikely.
    }


    protected Region getRegion(TableName tableName, byte[] row) throws IOException {
        byte[] regionName =
                regionServer.getConnection().locateRegion(tableName, row).getRegionInfo().getRegionName();
        return regionServer.getRegionByEncodedName(HRegionInfo.encodeRegionName(regionName));
    }

}
