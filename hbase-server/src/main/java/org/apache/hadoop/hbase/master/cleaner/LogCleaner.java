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
package org.apache.hadoop.hbase.master.cleaner;

import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;

import com.google.common.annotations.VisibleForTesting;

/**
 * This Chore, every time it runs, will attempt to delete the WALs in the old logs folder. The WAL
 * is only deleted if none of the cleaner delegates says otherwise.
 * @see BaseLogCleanerDelegate
 */
@InterfaceAudience.Private
public class LogCleaner extends CleanerChore<BaseLogCleanerDelegate> implements
    ConfigurationObserver {
  static final Log LOG = LogFactory.getLog(LogCleaner.class.getName());

  // Configuration key for log file queue size
  public final static String LOGFILE_DELETE_QUEUE_SIZE =
      "hbase.regionserver.logcleaner.queue.size";
  public final static int DEFAULT_LOGFILE_DELETE_QUEUE_SIZE = 1048576;

  // Configuration key for log file delete thread number
  public final static String LOGFILE_DELETE_THREAD_NUMBER =
      "hbase.regionserver.logcleaner.threadnumber";
  public final static int DEFAULT_LOGFILE_DELETE_THREAD_NUMBER = 1;

  private BlockingQueue<LogDeleteTask> deleteLogFileQueue;
  private int logFileQueueSize;
  private int logFileDeleteThreadNumber;
  private List<Thread> threads = new ArrayList<Thread>();
  private boolean running;

  private AtomicLong deletedLogFiles = new AtomicLong();
  final MetricsMaster metricsMaster;

  /**
   * @param p the period of time to sleep between each run
   * @param s the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldLogDir the path to the archived logs
   * @param metricsMaster master metrics to report old wal number
   */
  public LogCleaner(final int p, final Stoppable s, Configuration conf, FileSystem fs,
      Path oldLogDir, MetricsMaster metricsMaster) {
    super("LogsCleaner", p, s, conf, fs, oldLogDir, HBASE_MASTER_LOGCLEANER_PLUGINS);
    logFileQueueSize = conf.getInt(LOGFILE_DELETE_QUEUE_SIZE, DEFAULT_LOGFILE_DELETE_QUEUE_SIZE);
    deleteLogFileQueue = new LinkedBlockingQueue<LogDeleteTask>(logFileQueueSize);
    logFileDeleteThreadNumber =
        conf.getInt(LOGFILE_DELETE_THREAD_NUMBER, DEFAULT_LOGFILE_DELETE_THREAD_NUMBER);
    startLogFileDeleteThreads();
    this.metricsMaster = metricsMaster;
  }

  @Override
  protected boolean validate(Path file) {
    return DefaultWALProvider.validateWALFilename(file.getName());
  }

  @Override
  public int deleteFiles(Iterable<FileStatus> filesToDelete) {
    int deletedFiles = 0;
    List<LogDeleteTask> tasks = new ArrayList<LogDeleteTask>();
    // construct delete tasks and add into queue
    for (FileStatus file : filesToDelete) {
      LogDeleteTask task = deleteFile(file);
      if (task != null) {
        tasks.add(task);
      }
    }
    // wait for each submitted task to finish
    for (LogDeleteTask task : tasks) {
      if (task.getResult()) {
        deletedFiles++;
      }
    }
    return deletedFiles;
  }

  @Override
  public void cleanup() {
    super.cleanup();
    stopLogFileDeleteThreads();
  }

  private LogDeleteTask deleteFile(FileStatus file) {
    LogDeleteTask task = new LogDeleteTask(file);
    boolean enqueued = deleteLogFileQueue.offer(task);
    return enqueued ? task : null;
  }

  /**
   * Start threads for log file deletion
   */
  private void startLogFileDeleteThreads() {
    final String n = Thread.currentThread().getName();
    running = true;
    // start thread for log file deletion
    for (int i = 0; i < logFileDeleteThreadNumber; i++) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          consumerLoop(deleteLogFileQueue);
        }
      };
      thread.setDaemon(true);
      thread.setName(n + "-LogFileCleaner." + i + "-" + System.currentTimeMillis());
      thread.start();
      LOG.debug("Starting log file cleaner " + thread.getName());
      threads.add(thread);
    }
  }

  protected void consumerLoop(BlockingQueue<LogDeleteTask> queue) {
    try {
      while (running) {
        LogDeleteTask task = null;
        try {
          task = queue.take();
        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Interrupted while trying to take a task from queue", e);
          }
          break;
        }
        if (task != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Removing: " + task.filePath + " from archive");
          }
          boolean succeed;
          try {
            succeed = this.fs.delete(task.filePath, false);
          } catch (IOException e) {
            LOG.warn("Failed to delete file " + task.filePath, e);
            succeed = false;
          }
          task.setResult(succeed);
          if (succeed) {
            if (deletedLogFiles.get() == Long.MAX_VALUE) {
              LOG.info("Deleted more than Long.MAX_VALUE log files, reset counter to 0");
              deletedLogFiles = new AtomicLong();
            }
            deletedLogFiles.incrementAndGet();
          }
        }
      }
    } finally {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exit thread: " + Thread.currentThread());
      }
    }
  }

  /**
   * Stop threads for log file deletion
   */
  private void stopLogFileDeleteThreads() {
    running = false;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping log file delete threads");
    }
    for (Thread thread : threads) {
      thread.interrupt();
    }
  }

  class LogDeleteTask {
    private static final long MAX_WAIT = 60 * 1000L;
    private static final long WAIT_UNIT = 1000L;

    boolean done = false;
    boolean result;
    final Path filePath;
    final long fileLength;

    public LogDeleteTask(FileStatus file) {
      this.filePath = file.getPath();
      this.fileLength = file.getLen();
    }

    public synchronized void setResult(boolean result) {
      this.done = true;
      this.result = result;
      notify();
    }

    public synchronized boolean getResult() {
      long waitTime = 0;
      try {
        while (!done) {
          wait(WAIT_UNIT);
          waitTime += WAIT_UNIT;
          if (waitTime > MAX_WAIT) {
            LOG.warn("Wait more than " + MAX_WAIT + " ms for deleting " + this.filePath
                + ", exit...");
            return false;
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for result of deleting " + filePath
            + ", will return false", e);
        return false;
      }
      return this.result;
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    StringBuilder builder = new StringBuilder();
    builder.append("Updating configuration for LogCleaner, previous logFileQueueSize: ")
        .append(logFileQueueSize).append(", logFileDeleteThreadNumber: ")
        .append(logFileDeleteThreadNumber);
    stopLogFileDeleteThreads();
    this.logFileQueueSize =
        conf.getInt(LOGFILE_DELETE_QUEUE_SIZE, DEFAULT_LOGFILE_DELETE_QUEUE_SIZE);
    // record the left over tasks
    List<LogDeleteTask> leftOverTasks = new ArrayList<>();
    for (LogDeleteTask task : deleteLogFileQueue) {
      leftOverTasks.add(task);
    }
    deleteLogFileQueue = new LinkedBlockingQueue<LogDeleteTask>(logFileQueueSize);
    this.logFileDeleteThreadNumber =
        conf.getInt(LOGFILE_DELETE_THREAD_NUMBER, DEFAULT_LOGFILE_DELETE_THREAD_NUMBER);
    threads.clear();
    builder.append("; new logFileQueueSize: ").append(logFileQueueSize)
        .append(", logFileDeleteThreadNumber: ").append(logFileDeleteThreadNumber);
    LOG.debug(builder.toString());
    startLogFileDeleteThreads();
    // re-dispatch the left over tasks
    for (LogDeleteTask task : leftOverTasks) {
      deleteLogFileQueue.offer(task);
    }
  }

  @VisibleForTesting
  public List<Thread> getCleanerThreads() {
    return threads;
  }

  @VisibleForTesting
  public long getNumOfDeletedLogFiles() {
    return deletedLogFiles.get();
  }

  @VisibleForTesting
  public long getQueueSize() {
    return logFileQueueSize;
  }

  @Override
  protected void chore() {
    try {
      FileStatus[] files = FSUtils.listStatus(this.fs, this.oldFileDir);
      metricsMaster.updateOldWALNumber(files == null ? 0L : files.length);
      checkAndDeleteEntries(files);
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.warn("Error while cleaning the logs", e);
    }
  }
}
