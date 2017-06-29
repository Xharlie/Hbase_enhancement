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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EmbeddedReverseScanner extends EmbeddedSimpleScanner {
    private static final Log LOG = LogFactory.getLog(EmbeddedReverseScanner.class);

    public EmbeddedReverseScanner(Configuration conf, Scan scan, TableName tableName, HRegionServer regionServer)
            throws IOException {
        super(conf, scan, tableName, regionServer);
    }

    @Override
    protected boolean nextScanner() throws IOException {
        // Where to start the next scanner
        byte[] localStartKey;
        boolean locateTheClosestFrontRow = true;
        if (this.currentRegion != null) {
            byte[] startKey = this.currentRegion.getRegionInfo().getStartKey();
            if (startKey == null || Bytes.equals(startKey, HConstants.EMPTY_BYTE_ARRAY)
                    || checkScanStopRow(startKey)) {
                close();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Finished " + this.currentRegion);
                }
                return false;
            }
            localStartKey = startKey;
            if (LOG.isTraceEnabled()) {
                LOG.trace("Finished " + this.currentRegion);
            }
        } else {
            localStartKey = this.scan.getStartRow();
            if (!Bytes.equals(localStartKey, HConstants.EMPTY_BYTE_ARRAY)) {
                locateTheClosestFrontRow = false;
            }
        }

        if (LOG.isDebugEnabled() && this.currentRegion != null) {
            // Only worth logging if NOT first region in scan.
            LOG.trace(
                    "Advancing internal scanner to startKey at '" + Bytes
                        .toStringBinary(localStartKey) + "'");
        }

        try {
            byte[] locateStartRow = locateTheClosestFrontRow ? createClosestRowBefore(localStartKey) : null;
            if (locateStartRow == null) {
                // Just locate the region with the row
                this.currentRegion = (HRegion) getRegion(tableName, localStartKey);
                if (this.currentRegion == null) {
                    throw new IOException("Failed to find location, tableName="
                            + tableName + ", row=" + Bytes.toStringBinary(localStartKey));
                }
                scan.setStartRow(localStartKey);
            } else {
                List<Region> locatedRegions = locateRegionsInRange(locateStartRow, localStartKey);
                if (locatedRegions.isEmpty()) {
                    throw new DoNotRetryIOException(
                            "Does hbase:meta exist hole? Couldn't get regions for the range from "
                                    + Bytes.toStringBinary(locateStartRow) + " to "
                                    + Bytes.toStringBinary(localStartKey));
                }
                this.currentRegion = (HRegion) locatedRegions.get(locatedRegions.size() - 1);
                scan.setStartRow(locateStartRow);
            }

            this.regionScanner = this.currentRegion.getScanner(scan);
        } catch (IOException e) {
            close();
            throw e;
        }
        return true;
    }

    /**
     * TODO: copy from ReversedScannerCallable
     * Get the corresponding regions for an arbitrary range of keys.
     * @param startKey Starting row in range, inclusive
     * @param endKey Ending row in range, exclusive
     * @return A list of HRegionLocation corresponding to the regions that contain
     *         the specified range
     * @throws IOException
     */
    private List<Region> locateRegionsInRange(byte[] startKey, byte[] endKey) throws IOException {
        final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
        if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
            throw new IllegalArgumentException("Invalid range: "
                    + Bytes.toStringBinary(startKey) + " > "
                    + Bytes.toStringBinary(endKey));
        }
        List<Region> regionList = new ArrayList<Region>();
        byte[] currentKey = startKey;
        do {
            Region region = getRegion(tableName, currentKey);
            if (region != null && region.getRegionInfo().containsRow(currentKey)) {
                regionList.add(region);
            } else {
                throw new DoNotRetryIOException("Does hbase:meta exist hole? Locating row "
                        + Bytes.toStringBinary(currentKey) + " returns incorrect region "
                        + (region == null ? null : region.getRegionInfo()));
            }
            currentKey = region.getRegionInfo().getEndKey();
        } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
                && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
        return regionList;
    }

    /**
     * Create the closest row before the specified row
     * TODO: copy from ConnectionUtils, because the method is protected, change to public is ok?
     */
    static byte[] createClosestRowBefore(byte[] row) {
        byte[] MAX_BYTE_ARRAY = Bytes.createMaxByteArray(9);
        if (row.length == 0) {
            return MAX_BYTE_ARRAY;
        }
        if (row[row.length - 1] == 0) {
            return Arrays.copyOf(row, row.length - 1);
        } else {
            byte[] nextRow = new byte[row.length + MAX_BYTE_ARRAY.length];
            System.arraycopy(row, 0, nextRow, 0, row.length - 1);
            nextRow[row.length - 1] = (byte) ((row[row.length - 1] & 0xFF) - 1);
            System.arraycopy(MAX_BYTE_ARRAY, 0, nextRow, row.length, MAX_BYTE_ARRAY.length);
            return nextRow;
        }
    }

}
