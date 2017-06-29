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

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

/**
 * Manages the read/write consistency within memstore. This provides
 * an interface for readers to determine what entries to ignore, and
 * a mechanism for writers to obtain new write numbers, then "commit"
 * the new writes for readers to read (thus forming atomic transactions).
 */
@InterfaceAudience.Private
public class MultiVersionConsistencyControl {
  static final long NO_WRITE_NUMBER = 0;
  private AtomicLong memstoreRead = new AtomicLong(0);
  final AtomicLong writePoint = new AtomicLong(0);
  private final Object readWaiters = new Object();
  /**
   * Represents no value, or not set.
   */
  public static final long NONE = -1;

  // This is the pending queue of writes.
  private final LinkedList<WriteEntry> writeQueue =
      new LinkedList<WriteEntry>();

  /**
   * Default constructor. Initializes the memstoreRead/Write points to 0.
   */
  public MultiVersionConsistencyControl() {
  }

  /**
   * Construct and set read point. Write point is uninitialized.
   */
  public MultiVersionConsistencyControl(long startPoint) {
    tryAdvanceTo(startPoint, NONE);
  }

  /**
   * Call {@link #beginMemstoreInsert(Runnable)} with an empty {@link Runnable}.
   */
  public WriteEntry beginMemstoreInsert() {
    Runnable emptyAction = new Runnable() {
      @Override
      public void run() {
        // do nothing here
      }
    };
    return beginMemstoreInsert(emptyAction);
  }

  /**
   * Start a write transaction. Create a new {@link WriteEntry} with a new write number and add it
   * to our queue of ongoing writes. Return this WriteEntry instance.
   * <p>
   * To complete the write transaction and wait for it to be visible, call
   * {@link #completeMemstoreInsert(WriteEntry)}. If the write failed, call
   * {@link #advanceMemstore(WriteEntry)} so we can clean up AFTER removing ALL trace of the failed write
   * transaction.
   * <p>
   * The {@code action} will be executed under the lock which means it can keep the same order with
   * mvcc.
   * @see #completeMemstoreInsert(WriteEntry)
   * @see #advanceMemstore(WriteEntry)
   */
  public WriteEntry beginMemstoreInsert(Runnable action) {
    synchronized (writeQueue) {
      long nextWriteNumber = writePoint.incrementAndGet();
      WriteEntry e = new WriteEntry(nextWriteNumber);
      writeQueue.add(e);
      action.run();
      return e;
    }
  }

  /**
   * Complete a {@link WriteEntry} that was created by {@link #beginMemstoreInsert()}. At the
   * end of this call, the global read point is at least as large as the write point of the passed
   * in WriteEntry. Thus, the write is visible to MVCC readers.
   */
  public void completeMemstoreInsert(WriteEntry e) {
    waitForPreviousTransactionsComplete(e);
  }

  /**
   * Mark the {@link WriteEntry} as complete and advance the read point as much as possible.
   * Call this even if the write has FAILED (AFTER backing out the write transaction
   * changes completely) so we can clean up the outstanding transaction.
   * <p>
   * How much is the read point advanced?
   * Let S be the set of all write numbers that are completed and where all previous write numbers
   * are also completed.  Then, the read point is advanced to the supremum of S.
   *
   * @param e
   * @return true if e is visible to MVCC readers (that is, readpoint >= e.writeNumber)
   */
  public boolean advanceMemstore(WriteEntry e) {
    synchronized (writeQueue) {
      e.markCompleted();

      long nextReadValue = NONE;
      boolean ranOnce = false;
      while (!writeQueue.isEmpty()) {
        ranOnce = true;
        WriteEntry queueFirst = writeQueue.getFirst();

        if (nextReadValue > 0) {
          if (nextReadValue + 1 != queueFirst.getWriteNumber()) {
            throw new RuntimeException("Invariant in complete violated, nextReadValue="
                + nextReadValue + ", writeNumber=" + queueFirst.getWriteNumber());
          }
        }

        if (queueFirst.isCompleted()) {
          nextReadValue = queueFirst.getWriteNumber();
          writeQueue.removeFirst();
        } else {
          break;
        }
      }

      if (!ranOnce) {
        throw new RuntimeException("There is no first!");
      }

      if (nextReadValue > 0) {
        synchronized (readWaiters) {
          memstoreRead.set(nextReadValue);
          readWaiters.notifyAll();
        }
      }
      return memstoreRead.get() >= e.getWriteNumber();
    }
  }

  /**
   * Step the MVCC forward on to a new read/write basis.
   * @param newStartPoint
   */
  public void advanceTo(long newStartPoint) {
    while (true) {
      long seqId = this.getWritePoint();
      if (seqId >= newStartPoint) break;
      if (this.tryAdvanceTo(/* newSeqId = */ newStartPoint, /* expected = */ seqId)) break;
    }
  }

  /**
   * Step the MVCC forward on to a new read/write basis.
   * @param newStartPoint Point to move read and write points to.
   * @param expected If not -1 (#NONE)
   * @return Returns false if <code>expected</code> is not equal to the current
   *         <code>readPoint</code> or if <code>startPoint</code> is less than current
   *         <code>readPoint</code>
   */
  boolean tryAdvanceTo(long newStartPoint, long expected) {
    synchronized (writeQueue) {
      long currentRead = this.memstoreRead.get();
      long currentWrite = this.writePoint.get();
      if (currentRead != currentWrite) {
        throw new RuntimeException("Already used this mvcc; currentRead=" + currentRead
            + ", currentWrite=" + currentWrite + "; too late to tryAdvanceTo");
      }
      if (expected != NONE && expected != currentRead) {
        return false;
      }

      if (newStartPoint < currentRead) {
        return false;
      }

      memstoreRead.set(newStartPoint);
      writePoint.set(newStartPoint);
    }
    return true;
  }

  /**
   * Wait for all previous MVCC transactions complete
   */
  public void waitForPreviousTransactionsComplete() {
    completeMemstoreInsert(beginMemstoreInsert());
  }

  public void waitForPreviousTransactionsComplete(WriteEntry e) {
    if (!advanceMemstore(e)) {
      // only wait when failed to advance read point to given WriteEntry
      waitForRead(e);
    }
  }

  /**
   * Wait for the global readPoint to advance upto the specified transaction number.
   */
  public void waitForRead(WriteEntry e) {
    boolean interrupted = false;
    synchronized (readWaiters) {
      while (memstoreRead.get() < e.getWriteNumber()) {
        try {
          readWaiters.wait(0);
        } catch (InterruptedException ie) {
          // We were interrupted... finish the loop -- i.e. cleanup --and then
          // on our way out, reset the interrupt flag.
          interrupted = true;
        }
      }
    }
    if (interrupted) Thread.currentThread().interrupt();
  }

  public long memstoreReadPoint() {
    return memstoreRead.get();
  }

  @VisibleForTesting
  public long getWritePoint() {
    return writePoint.get();
  }

  @VisibleForTesting
  public String toString() {
    return Objects.toStringHelper(this)
        .add("readPoint", memstoreReadPoint())
        .add("writePoint", writePoint).toString();
  }

  public static class WriteEntry {
    private long writeNumber;
    private volatile boolean completed = false;

    WriteEntry(long writeNumber) {
      this.writeNumber = writeNumber;
    }

    void markCompleted() {
      this.completed = true;
    }

    boolean isCompleted() {
      return this.completed;
    }

    public long getWriteNumber() {
      return this.writeNumber;
    }

    @Override
    public String toString() {
      return this.writeNumber + ", " + this.completed;
    }
  }

  public static final long FIXED_SIZE = ClassSize.align(
      ClassSize.OBJECT +
      2 * Bytes.SIZEOF_LONG +
      2 * ClassSize.REFERENCE);

}
