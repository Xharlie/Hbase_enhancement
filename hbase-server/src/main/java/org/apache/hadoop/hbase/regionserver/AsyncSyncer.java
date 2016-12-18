package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.Trace;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by xharlie on 11/28/16.
 */
public class AsyncSyncer extends HasThread {
  public static final Log LOG = LogFactory.getLog(AsyncSyncer.class);
  private long lastSyncedTxid = 0;
  private volatile boolean isSyncing = false;
  private Object syncLock = new Object();
  public List<Pair<HRegion.BatchOperationInProgress<?>,ResponseToolKit>> pendingSyncs;
  public final Object pendingSyncsLock = new Object();
  public HRegion hRegion;

  public AsyncSyncer(String name, HRegion hRegion) {
    super(name);
    this.hRegion = hRegion;
    pendingSyncs = new LinkedList<Pair<HRegion.BatchOperationInProgress<?>,ResponseToolKit>>();
  }

  public boolean isSyncing() {
    return this.isSyncing;
  }

  // wake up (called by AsyncWriter thread) AsyncSyncer thread
  // to sync(flush) writes written by AsyncWriter in HDFS
  public void notifySyncLock () {
      this.syncLock.notify();
  }

  public void run() {
    try {
      while (!this.isInterrupted()) {
        // 1. wait until AsyncWriter has written data to HDFS and
        //    called setWrittenTxid to wake up us
        synchronized (this.syncLock) {
          while (pendingSyncs.size() == 0) {
            this.isSyncing = false;
            this.syncLock.wait();
          }
        }
        this.isSyncing = true;
        try{
          List<Pair<HRegion.BatchOperationInProgress<?>,ResponseToolKit>> pendSyncs = null;
          synchronized (pendingSyncsLock) {
            pendSyncs = this.pendingSyncs;
            pendingSyncs = new LinkedList<Pair<HRegion.BatchOperationInProgress<?>,ResponseToolKit>>();
          }
          while (pendSyncs.size() != 0) {
            HRegion.BatchOperationInProgress batchOp = pendSyncs.get(pendSyncs.size() - 1).getFirst();
            ResponseToolKit responseTK = pendSyncs.get(pendSyncs.size() - 1).getSecond();
            try {
              long newSize = syncOrSkip(batchOp, responseTK);
              if (hRegion.isFlushSize(newSize)) {   // TODO find a way to not change access level
                hRegion.requestFlush();
              }
            } catch (Throwable e) {
              responseTK.errorThrowable = e;
              responseTK.error = StringUtils.stringifyException(e);
            } finally {
              responseTK.respond(batchOp.retCodeDetails, responseTK.errorThrowable, responseTK.error);
            }
          }
        } catch (IOException e) {
          LOG.fatal("Error while AsyncSyncer sync, request close of hlog ", e);
          this.isSyncing = false;
        }
      }
    } catch (InterruptedException e) {
      LOG.debug(getName() + " interrupted while waiting for " +
              "notification from AsyncWriter thread");
    } catch (Exception e) {
      LOG.error("UNEXPECTED", e);
    } finally {
      LOG.info(getName() + " exiting");
    }
  }

  public long syncOrSkip(HRegion.BatchOperationInProgress<?> batchOp, ResponseToolKit responseTK ) throws IOException {
    if (responseTK.txid <= lastSyncedTxid) {
      return responseTK.addedSize;
    } else {
      long newSize = hRegion.addAndGetGlobalMemstoreSize(hRegion.syncAndResponse(batchOp, responseTK));
      lastSyncedTxid = responseTK.txid;
      return newSize;
    }
  }
}
