/*
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.regionserver.controller.ThroughputController;

/**
 * Store flusher interface. Turns a snapshot of memstore into a set of store files (usually one).
 * Custom implementation can be provided.
 */
@InterfaceAudience.Private
abstract class StoreFlusher {
  protected Configuration conf;
  protected Store store;

  public StoreFlusher(Configuration conf, Store store) {
    this.conf = conf;
    this.store = store;
  }

  /**
   * Turns a snapshot of memstore into a set of store files.
   * @param snapshot Memstore snapshot.
   * @param cacheFlushSeqNum Log cache flush sequence number.
   * @param status Task that represents the flush operation and may be updated with status.
   * @param throughputController A controller to avoid flush too fast
   * @return List of files written. Can be empty; must not be null.
   */
  public abstract List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
      MonitoredTask status, ThroughputController throughputController) throws IOException;

  protected void finalizeWriter(StoreFile.Writer writer, long cacheFlushSeqNum,
      MonitoredTask status) throws IOException {
    // Write out the log sequence number that corresponds to this output
    // hfile. Also write current time in metadata as minFlushTime.
    // The hfile is current up to and including cacheFlushSeqNum.
    status.setStatus("Flushing " + store + ": appending metadata");
    writer.appendMetadata(cacheFlushSeqNum, false);
    status.setStatus("Flushing " + store + ": closing flushed file");
    writer.close();
  }


  /**
   * Creates the scanner for flushing snapshot. Also calls coprocessors.
   * @param snapshotScanner
   * @param smallestReadPoint
   * @return The scanner; null if coprocessor is canceling the flush.
   */
  protected InternalScanner createScanner(KeyValueScanner snapshotScanner,
      long smallestReadPoint) throws IOException {
    InternalScanner scanner = null;
    if (store.getCoprocessorHost() != null) {
      scanner = store.getCoprocessorHost().preFlushScannerOpen(store, snapshotScanner);
    }
    if (scanner == null) {
      Scan scan = new Scan();
      scan.setMaxVersions(store.getScanInfo().getMaxVersions());
      scanner = new StoreScanner(store, store.getScanInfo(), scan,
          Collections.singletonList(snapshotScanner), ScanType.COMPACT_RETAIN_DELETES,
          smallestReadPoint, HConstants.OLDEST_TIMESTAMP);
    }
    assert scanner != null;
    if (store.getCoprocessorHost() != null) {
      try {
        return store.getCoprocessorHost().preFlush(store, scanner);
      } catch (IOException ioe) {
        scanner.close();
        throw ioe;
      }
    }
    return scanner;
  }

  /**
   * Performs memstore flush, writing data from scanner into sink.
   * @param scanner Scanner to get data from.
   * @param sink Sink to write data to. Could be StoreFile.Writer.
   * @param smallestReadPoint Smallest read point used for the flush.
   * @param throughputController A controller to avoid flush too fast
   */
  protected void performFlush(InternalScanner scanner, CellSink sink,
      long smallestReadPoint, ThroughputController throughputController) throws IOException {
    int compactionKVMax =
      conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);

    ScannerContext scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    List<Cell> kvs = new ArrayList<Cell>();
    boolean hasMore;
    String flushName = generateFlushName();
    // no control on meta or system table flush
    boolean control =
        throughputController != null && !store.getRegionInfo().isMetaTable()
            && !store.getRegionInfo().isSystemTable();
    if (control) throughputController.start(flushName);
    try {
      do {
        hasMore = scanner.next(kvs, scannerContext);
        if (!kvs.isEmpty()) {
          for (Cell c : kvs) {
            // If we know that this KV is going to be included always, then let us
            // set its memstoreTS to 0. This will help us save space when writing to
            // disk.
            sink.append(c);
            int len = KeyValueUtil.length(c);
            if (control) throughputController.control(flushName, len);
          }
          kvs.clear();
        }
      } while (hasMore);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while control throughput of flushing "
          + flushName);
    } finally {
      if (control) throughputController.finish(flushName);
    }
  }

  /**
   * Used to prevent flush name conflict when multiple flushes running parallel on the same store.
   */
  private static final AtomicInteger NAME_COUNTER = new AtomicInteger(0);

  private String generateFlushName() {
    int counter;
    for (;;) {
      counter = NAME_COUNTER.get();
      int next = counter == Integer.MAX_VALUE ? 0 : counter + 1;
      if (NAME_COUNTER.compareAndSet(counter, next)) {
        break;
      }
    }
    return store.getRegionInfo().getRegionNameAsString() + "#"
        + store.getFamily().getNameAsString() + "#flush#" + counter;
  }
}
