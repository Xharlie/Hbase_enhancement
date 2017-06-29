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
package org.apache.hadoop.hbase.regionserver;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;

import java.io.IOException;

/**
 * ImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and is not needed for a {@link MutableSegment}. Specifically, the method
 * {@link ImmutableSegment#getKeyValueScanner()} builds a special scanner for the
 * {@link MemStoreSnapshot} object.
 */
@InterfaceAudience.Private
public class ImmutableSegment extends Segment {

  private static final long DEEP_OVERHEAD = Segment.DEEP_OVERHEAD
      + ClassSize.REFERENCE // Refs to type
      + ClassSize.TIMERANGE;
  public static final long DEEP_OVERHEAD_CSLM = DEEP_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP;

  /**
   * Types of ImmutableSegment
   */
  public enum Type {
    SKIPLIST_MAP_BASED,
  }

  private Type type = Type.SKIPLIST_MAP_BASED;

  /////////////////////  CONSTRUCTORS  /////////////////////
  /**------------------------------------------------------------------------
   * Copy C-tor to be used when new ImmutableSegment is being built from a Mutable one.
   * This C-tor should be used when active MutableSegment is pushed into the compaction
   * pipeline and becomes an ImmutableSegment.
   */
  protected ImmutableSegment(Segment segment) {
    super(segment);
    this.type = Type.SKIPLIST_MAP_BASED;
  }

  /////////////////////  PUBLIC METHODS  /////////////////////
  /**
   * Builds a special scanner for the MemStoreSnapshot object that is different than the
   * general segment scanner.
   * @return a special scanner for the MemStoreSnapshot object
   */
  public KeyValueScanner getKeyValueScanner() {
    return new CollectionBackedScanner(getCellSet(), getComparator());
  }

  @Override
  protected long heapSizeChange(Cell cell, boolean succ) {
    if (succ) {
      switch (this.type) {
        case SKIPLIST_MAP_BASED:
          return super.heapSizeChange(cell, succ);
        default:
          throw new RuntimeException("Unknown type " + type);
      }
    }
    return 0;
  }

  /**
   * Removes the given cell from the segment
   * @return the change in the heap size
   */
  public long rollback(Cell cell) {
    Cell found = getCellSet().get(cell);
    if (found != null && found.getSequenceId() == cell.getSequenceId()) {
      getCellSet().remove(cell);
      int cellLen = getCellLength(cell);
      long heapSize = heapSizeChange(cell, true);
      incSize(-cellLen, -heapSize);
      return heapSize;
    }
    return 0;
  }
}
