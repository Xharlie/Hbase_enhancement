package org.apache.hadoop.hbase.util;


import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.lang.Thread.UncaughtExceptionHandler;
/**
* Abstract class which contains Threads and delegates the common Thread
* methods to that instance.
*
* The purpose of this class is to generalize the class of HasThread ,
* which is to workaround Sun JVM bug #6915621, in which
* something internal to the JDK uses Thread.currentThread() as a monitor
* lock. This can produce deadlocks like HBASE-4367, HBASE-4101, etc.
*/
@InterfaceAudience.Private
public abstract class HasThreads {
  private Thread[] threads;
  public Object[] locks;

  public HasThreads() {
    this(Thread.currentThread().getName() + ".HasThreads");
  }

  public HasThreads(String name) {
    this(name, 1);
  }

  public HasThreads(String name, int workerNum) {
    this.threads = new Thread[workerNum];
    this.locks = new Object[workerNum];
    for (int i = 0; i < workerNum; i++) {
      final int index = i;
      this.locks[i] = new Object();
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          runTask(index);
        }
      }, name + ".worker" + i);
    }
  }

  public abstract void runTask(int index);

  public Thread getThread(int index) {
    return threads[index];
  }

  //// Begin delegation to Thread

  public final String getName(int index) {
    return threads[index].getName();
  }

  public void interrupt(int index) {
    threads[index].interrupt();
  }

  public void interruptAll() {
    for (int i = 0; i < threads.length; i++) {
      threads[i].interrupt();
    }
  }

  public final boolean isAlive(int index) {
    return threads[index].isAlive();
  }

  public boolean isInterrupted(int index) {
    return threads[index].isInterrupted();
  }

  public boolean hasInterrupted() {
    for (int i = 0; i < threads.length; i++) {
      if(threads[i].isInterrupted()) return true;
    }
    return false;
  }

  public boolean allInterrupted() {
    for (int i = 0; i < threads.length; i++) {
      if(!threads[i].isInterrupted()) return false;
    }
    return true;
  }

  public final void setDaemon(int index, boolean on) {
    threads[index].setDaemon(on);
  }

  public final void setName(int index, String name) {
    threads[index].setName(name);
  }

  public final void setPriority(int index, int newPriority) {
    threads[index].setPriority(newPriority);
  }

  public void setUncaughtExceptionHandler(int index, UncaughtExceptionHandler eh) {
    threads[index].setUncaughtExceptionHandler(eh);
  }

  public void start(int index) {
    threads[index].start();
  }

  public void startAll() {
    for (int i = 0; i < threads.length; i++) {
      threads[i].start();
    }
  }

  public final void join(int index) throws InterruptedException {
    threads[index].join();
  }

  public final void join(int index, long millis, int nanos) throws InterruptedException {
    threads[index].join(millis, nanos);
  }

  public final void join(int index,long millis) throws InterruptedException {
    threads[index].join(millis);
  }

  public final void joinAll() throws InterruptedException {
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
  }

  public final void joinAll(long millis, int nanos) throws InterruptedException {
    for (int i = 0; i < threads.length; i++) {
      threads[i].join(millis, nanos);
    }
  }

  public final void joinAll(long millis) throws InterruptedException {
    for (int i = 0; i < threads.length; i++) {
      threads[i].join(millis);
    }
  }
  //// End delegation to Thread
}
