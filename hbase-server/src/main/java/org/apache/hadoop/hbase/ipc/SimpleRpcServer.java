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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslDigestCallbackHandler;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.TraceInfo;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.BlockingService;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

/**
 * An RPC server that hosts protobuf described Services.
 *
 * An RpcServer instance has a Listener that hosts the socket.  Listener has fixed number
 * of Readers in an ExecutorPool, 10 by default.  The Listener does an accept and then
 * round robin a Reader is chosen to do the read.  The reader is registered on Selector.  Read does
 * total read off the channel and the parse from which it makes a Call.  The call is wrapped in a
 * CallRunner and passed to the scheduler to be run.  Reader goes back to see if more to be done
 * and loops till done.
 *
 * <p>Scheduler can be variously implemented but default simple scheduler has handlers to which it
 * has given the queues into which calls (i.e. CallRunner instances) are inserted.  Handlers run
 * taking from the queue.  They run the CallRunner#run method on each item gotten from queue
 * and keep taking while the server is up.
 *
 * CallRunner#run executes the call.  When done, asks the included Call to put itself on new
 * queue for Responder to pull from and return result to client.
 *
 * @see RpcClientImpl
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class SimpleRpcServer extends RpcServer {

  private static final String WARN_DELAYED_CALLS = "hbase.ipc.warn.delayedrpc.number";

  private static final int DEFAULT_WARN_DELAYED_CALLS = 1000;

  private final int warnDelayedCalls;

  private AtomicInteger delayedCalls;

  protected final InetSocketAddress bindAddress;
  protected int port;                             // port we listen on
  protected InetSocketAddress address;            // inet address we listen on
  private int readThreads;                        // number of read threads
  protected int maxIdleTime;                      // the maximum idle time after
                                                  // which a client may be
                                                  // disconnected
  protected int thresholdIdleConnections;         // the number of idle
                                                  // connections after which we
                                                  // will start cleaning up idle
                                                  // connections
  int maxConnectionsToNuke;                       // the max number of
                                                  // connections to nuke
                                                  // during a cleanup

  protected int socketSendBufferSize;
  protected final boolean tcpNoDelay;   // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives
  protected final long purgeTimeout;    // in milliseconds

  /** A running count of the size of response queues of all active connections */
  protected final Counter responseQueueSize = new Counter();

  /** Counter of the length of response queues of all active connections */
  protected final AtomicInteger responseQueueLength = new AtomicInteger(0);

  protected final List<Connection> connectionList =
    Collections.synchronizedList(new LinkedList<Connection>());
  //maintain a list
  //of client connections
  private Listener listener = null;
  protected Responder responder = null;
  protected int numConnections = 0;

  /**
   * Datastructure that holds all necessary to a method invocation and then afterward, carries
   * the result.
   */
  class Call extends RpcServer.Call {

    protected boolean delayResponse;
    protected Responder responder;
    protected boolean delayReturnValue;           // if the return value should be
                                                  // set at call completion

    Call(int id, final BlockingService service, final MethodDescriptor md, RequestHeader header,
         Message param, CellScanner cellScanner, Connection connection, Responder responder,
         long size, TraceInfo tinfo, final InetAddress remoteAddress) {
      super(id, service, md, header, param, cellScanner, connection, size, tinfo, remoteAddress);
      this.delayResponse = false;
      this.responder = responder;
    }
    
    Connection getConnection() {
      return (Connection) this.connection;
    }

    /**
     * Call is done. Execution happened and we returned results to client. It is now safe to
     * cleanup.
     */
    void done() {
      super.done();
      this.getConnection().decRpcCount();  // Say that we're done with this call.
    }

    @Override
    public synchronized void endDelay(Object result) throws IOException {
      assert this.delayResponse;
      assert this.delayReturnValue || result == null;
      this.delayResponse = false;
      delayedCalls.decrementAndGet();
      if (this.delayReturnValue) {
        this.setResponse((Message)result, null, null, null);
      }
      this.responder.doRespond(this);
    }

    @Override
    public synchronized void endDelay() throws IOException {
      this.endDelay(null);
    }

    @Override
    public synchronized void startDelay(boolean delayReturnValue) {
      assert !this.delayResponse;
      this.delayResponse = true;
      this.delayReturnValue = delayReturnValue;
      int numDelayed = delayedCalls.incrementAndGet();
      if (numDelayed > warnDelayedCalls) {
        LOG.warn("Too many delayed calls: limit " + warnDelayedCalls + " current " + numDelayed);
      }
    }

    @Override
    public synchronized void endDelayThrowing(Throwable t) throws IOException {
      this.setResponse(null, null, t, StringUtils.stringifyException(t));
      this.delayResponse = false;
      this.sendResponseIfReady();
    }

    @Override
    public synchronized boolean isDelayed() {
      return this.delayResponse;
    }

    @Override
    public synchronized boolean isReturnValueDelayed() {
      return this.delayReturnValue;
    }

    @Override
    public boolean isClientCellBlockSupport() {
      return this.connection != null && this.connection.codec != null;
    }

    public long getSize() {
      return this.size;
    }

    /**
     * If we have a response, and delay is not set, then respond
     * immediately.  Otherwise, do not respond to client.  This is
     * called by the RPC code in the context of the Handler thread.
     */
    public synchronized void sendResponseIfReady() throws IOException {
      if (!this.delayResponse) {
        this.responder.doRespond(this);
      }
    }

  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {

    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
    private Reader[] readers = null;
    private int currentReader = 0;
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private long cleanupInterval = 10000; //the minimum interval between
                                          //two cleanup runs
    private int backlogLength;

    private ExecutorService readPool;

    public Listener(final String name) throws IOException {
      super(name);
      backlogLength = conf.getInt("hbase.ipc.server.listen.queue.size", 128);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the binding addrees (can be different from the default interface)
      bind(acceptChannel.socket(), bindAddress, backlogLength);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      address = (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
      // create a selector;
      selector= Selector.open();

      readers = new Reader[readThreads];
      readPool = Executors.newFixedThreadPool(readThreads,
        new ThreadFactoryBuilder().setNameFormat(
          "RpcServer.reader=%d,bindAddress=" + bindAddress.getHostName() +
          ",port=" + port).setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
      for (int i = 0; i < readThreads; ++i) {
        Reader reader = new Reader();
        readers[i] = reader;
        readPool.execute(reader);
      }
      LOG.info(getName() + ": started " + readThreads + " reader(s).");

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("RpcServer.listener,port=" + port);
      this.setDaemon(true);
    }


    private class Reader implements Runnable {
      private volatile boolean adding = false;
      private final Selector readSelector;

      Reader() throws IOException {
        this.readSelector = Selector.open();
      }
      @Override
      public void run() {
        try {
          doRunLoop();
        } finally {
          try {
            readSelector.close();
          } catch (IOException ioe) {
            LOG.error(getName() + ": error closing read selector in " + getName(), ioe);
          }
        }
      }

      private synchronized void doRunLoop() {
        while (running) {
          try {
            readSelector.select();
            while (adding) {
              this.wait(1000);
            }

            Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              iter.remove();
              if (key.isValid()) {
                if (key.isReadable()) {
                  doRead(key);
                }
              }
            }
          } catch (InterruptedException e) {
            LOG.debug("Interrupted while sleeping");
            return;
          } catch (IOException ex) {
            LOG.info(getName() + ": IOException in Reader", ex);
          }
        }
      }

      /**
       * This gets reader into the state that waits for the new channel
       * to be registered with readSelector. If it was waiting in select()
       * the thread will be woken up, otherwise whenever select() is called
       * it will return even if there is nothing to read and wait
       * in while(adding) for finishAdd call
       */
      public void startAdd() {
        adding = true;
        readSelector.wakeup();
      }

      public synchronized SelectionKey registerChannel(SocketChannel channel)
        throws IOException {
        return channel.register(readSelector, SelectionKey.OP_READ);
      }

      public synchronized void finishAdd() {
        adding = false;
        this.notify();
      }
    }

    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all
     * connections will be looked at for the cleanup.
     * @param force all connections will be looked at for cleanup
     */
    private void cleanupConnections(boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        long currentTime = System.currentTimeMillis();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        int start = 0;
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        int i = start;
        int numNuked = 0;
        while (i <= end) {
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            //noinspection UnusedAssignment
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      while (running) {
        SelectionKey key = null;
        try {
          selector.select(); // FindBugs IS2_INCONSISTENT_SYNC
          Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
              }
            } catch (IOException ignored) {
              if (LOG.isTraceEnabled()) LOG.trace("ignored", ignored);
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OutOfMemoryError");
              closeCurrentConnection(key, e);
              cleanupConnections(true);
              return;
            }
          } else {
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            LOG.warn(getName() + ": OutOfMemoryError in server select", e);
            closeCurrentConnection(key, e);
            cleanupConnections(true);
            try {
              Thread.sleep(60000);
            } catch (InterruptedException ex) {
              LOG.debug("Interrupted while sleeping");
              return;
            }
          }
        } catch (Exception e) {
          closeCurrentConnection(key, e);
        }
        cleanupConnections(false);
      }

      LOG.info(getName() + ": stopping");

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException ignored) {
          if (LOG.isTraceEnabled()) LOG.trace("ignored", ignored);
        }

        selector= null;
        acceptChannel= null;

        // clean up all connections
        while (!connectionList.isEmpty()) {
          closeConnection(connectionList.remove(0));
        }
      }
    }

    private void closeCurrentConnection(SelectionKey key, Throwable e) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress() +
                (e != null ? " on error " + e.getMessage() : ""));
          }
          closeConnection(c);
          key.attach(null);
        }
      }
    }

    InetSocketAddress getAddress() {
      return address;
    }

    void doAccept(SelectionKey key) throws IOException, OutOfMemoryError {
      Connection c;
      ServerSocketChannel server = (ServerSocketChannel) key.channel();

      SocketChannel channel;
      while ((channel = server.accept()) != null) {
        try {
          channel.configureBlocking(false);
          channel.socket().setTcpNoDelay(tcpNoDelay);
          channel.socket().setKeepAlive(tcpKeepAlive);
        } catch (IOException ioe) {
          channel.close();
          throw ioe;
        }

        Reader reader = getReader();
        try {
          reader.startAdd();
          SelectionKey readKey = reader.registerChannel(channel);
          c = getConnection(channel, System.currentTimeMillis());
          readKey.attach(c);
          synchronized (connectionList) {
            connectionList.add(numConnections, c);
            numConnections++;
          }
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": connection from " + c.toString() +
                "; # active connections: " + numConnections);
        } finally {
          reader.finishAdd();
        }
      }
    }

    void doRead(SelectionKey key) throws InterruptedException {
      int count;
      Connection c = (Connection) key.attachment();
      if (c == null) {
        return;
      }
      c.setLastContact(System.currentTimeMillis());
      try {
        count = c.readAndProcess();

        if (count > 0) {
          c.setLastContact(System.currentTimeMillis());
        }

      } catch (InterruptedException ieo) {
        throw ieo;
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": Caught exception while reading:", e);
        }
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": DISCONNECTING client " + c.toString() +
              " because read count=" + count +
              ". Number of active connections: " + numConnections);
        }
        closeConnection(c);
      }
    }

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {
          LOG.info(getName() + ": exception in closing listener socket. " + e);
        }
      }
      readPool.shutdownNow();
    }

    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  // Sends responses of RPC back to clients.
  protected class Responder extends Thread {
    private final Selector writeSelector;
    private final Set<Connection> writingCons =
        Collections.newSetFromMap(new ConcurrentHashMap<Connection, Boolean>());

    Responder() throws IOException {
      this.setName("RpcServer.responder");
      this.setDaemon(true);
      this.setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER);
      writeSelector = Selector.open(); // create a selector
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      try {
        doRunLoop();
      } finally {
        LOG.info(getName() + ": stopping");
        try {
          writeSelector.close();
        } catch (IOException ioe) {
          LOG.error(getName() + ": couldn't close write selector", ioe);
        }
      }
    }

    /**
     * Take the list of the connections that want to write, and register them
     * in the selector.
     */
    private void registerWrites() {
      Iterator<Connection> it = writingCons.iterator();
      while (it.hasNext()) {
        Connection c = it.next();
        it.remove();
        SelectionKey sk = c.channel.keyFor(writeSelector);
        try {
          if (sk == null) {
            try {
              c.channel.register(writeSelector, SelectionKey.OP_WRITE, c);
            } catch (ClosedChannelException e) {
              // ignore: the client went away.
              if (LOG.isTraceEnabled()) LOG.trace("ignored", e);
            }
          } else {
            sk.interestOps(SelectionKey.OP_WRITE);
          }
        } catch (CancelledKeyException e) {
          // ignore: the client went away.
          if (LOG.isTraceEnabled()) LOG.trace("ignored", e);
        }
      }
    }

    /**
     * Add a connection to the list that want to write,
     */
    public void registerForWrite(Connection c) {
      if (writingCons.add(c)) {
        writeSelector.wakeup();
      }
    }

    private void doRunLoop() {
      long lastPurgeTime = 0;   // last check for old calls.
      while (running) {
        try {
          registerWrites();
          int keyCt = writeSelector.select(purgeTimeout);
          if (keyCt == 0) {
            continue;
          }

          Set<SelectionKey> keys = writeSelector.selectedKeys();
          Iterator<SelectionKey> iter = keys.iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.debug(getName() + ": asyncWrite", e);
            }
          }

          lastPurgeTime = purge(lastPurgeTime);

        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OutOfMemoryError");
              return;
            }
          } else {
            //
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            //
            LOG.warn(getName() + ": OutOfMemoryError in server select", e);
            try {
              Thread.sleep(60000);
            } catch (InterruptedException ex) {
              LOG.debug("Interrupted while sleeping");
              return;
            }
          }
        } catch (Exception e) {
          LOG.warn(getName() + ": exception in Responder " +
              StringUtils.stringifyException(e), e);
        }
      }
      LOG.info(getName() + ": stopped");
    }

    /**
     * If there were some calls that have not been sent out for a
     * long time, we close the connection.
     * @return the time of the purge.
     */
    private long purge(long lastPurgeTime) {
      long now = System.currentTimeMillis();
      if (now < lastPurgeTime + purgeTimeout) {
        return lastPurgeTime;
      }

      ArrayList<Connection> conWithOldCalls = new ArrayList<Connection>();
      // get the list of channels from list of keys.
      synchronized (writeSelector.keys()) {
        for (SelectionKey key : writeSelector.keys()) {
          Connection connection = (Connection) key.attachment();
          if (connection == null) {
            throw new IllegalStateException("Coding error: SelectionKey key without attachment.");
          }
          Call call = connection.responseQueue.peekFirst();
          if (call != null && now > call.timestamp + purgeTimeout) {
            conWithOldCalls.add(call.getConnection());
          }
        }
      }

      // Seems safer to close the connection outside of the synchronized loop...
      for (Connection connection : conWithOldCalls) {
        closeConnection(connection);
      }

      return now;
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Connection connection = (Connection) key.attachment();
      if (connection == null) {
        throw new IOException("doAsyncWrite: no connection");
      }
      if (key.channel() != connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      if (processAllResponses(connection)) {
        try {
          // We wrote everything, so we don't need to be told when the socket is ready for
          //  write anymore.
         key.interestOps(0);
        } catch (CancelledKeyException e) {
          /* The Listener/reader might have closed the socket.
           * We don't explicitly cancel the key, so not sure if this will
           * ever fire.
           * This warning could be removed.
           */
          LOG.warn("Exception while changing ops : " + e);
        }
      }
    }

    /**
     * Process the response for this call. You need to have the lock on
     * {@link org.apache.hadoop.hbase.ipc.SimpleRpcServer.Connection#responseWriteLock}
     *
     * @param call the call
     * @param inQueue true if the call is from response queue
     * @return true if we proceed the call fully, false otherwise.
     * @throws IOException
     */
    private boolean processResponse(final Call call, final boolean inQueue) throws IOException {
      boolean error = true;
      try {
        // Send as much data as we can in the non-blocking fashion
        long numBytes = channelWrite(call.getConnection().channel, call.response);
        if (numBytes < 0) {
          throw new HBaseIOException("Error writing on the socket " +
            "for the call:" + call.toShortString());
        }
        error = false;
        if (inQueue) {
          responseQueueSize.add(-1 * numBytes);
        }
      } finally {
        if (error) {
          LOG.debug(getName() + call.toShortString() + ": output error -- closing");
          closeConnection(call.getConnection());
        }
      }

      if (!call.response.hasRemaining()) {
        call.done();
        return true;
      } else {
        return false; // Socket can't take more, we will have to come back.
      }
    }

    /**
     * Process all the responses for this connection
     *
     * @return true if all the calls were processed or that someone else is doing it.
     * false if there * is still some work to do. In this case, we expect the caller to
     * delay us.
     * @throws IOException
     */
    private boolean processAllResponses(final Connection connection) throws IOException {
      // We want only one writer on the channel for a connection at a time.
      connection.responseWriteLock.lock();
      try {
        for (int i = 0; i < 20; i++) {
          // protection if some handlers manage to need all the responder
          Call call = connection.responseQueue.pollFirst();
          if (call == null) {
            return true;
          }
          // only decrease the response queue length when there's a call there
          responseQueueLength.decrementAndGet();
          if (!processResponse(call, true)) {
            // Increase response queue length first before enqueue to avoid negative length
            responseQueueLength.incrementAndGet();
            connection.responseQueue.addFirst(call);
            // no need to increase response queue size for non-first enqueue
            return false;
          }
        }
      } finally {
        connection.responseWriteLock.unlock();
      }

      return connection.responseQueue.isEmpty();
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException {
      boolean added = false;

      // If there is already a write in progress, we don't wait. This allows to free the handlers
      //  immediately for other tasks.
      if (call.getConnection().responseQueue.isEmpty() && call.getConnection().responseWriteLock.tryLock()) {
        try {
          if (call.getConnection().responseQueue.isEmpty()) {
            // If we're alone, we can try to do a direct call to the socket. It's
            //  an optimisation to save on context switches and data transfer between cores..
            if (processResponse(call, false)) {
              return; // we're done.
            }
            int responseSize = call.response.getRemaining();
            // Increase response queue length first before enqueue to avoid negative length
            responseQueueLength.incrementAndGet();
            // Too big to fit, putting ahead.
            call.getConnection().responseQueue.addFirst(call);
            // Increase response queue size at the first enqueue
            responseQueueSize.add(responseSize);
            added = true; // We will register to the selector later, outside of the lock.
          }
        } finally {
          call.getConnection().responseWriteLock.unlock();
        }
      }

      if (!added) {
        // response.remaining will change when processed, so get size first before enqueue
        int responseSize = call.response.getRemaining();
        // Increase response queue length first before enqueue to avoid negative length
        responseQueueLength.incrementAndGet();
        call.getConnection().responseQueue.addLast(call);
        // Increase response queue size at the first enqueue
        responseQueueSize.add(responseSize);
      }
      call.responder.registerForWrite(call.getConnection());

      // set the serve time when the response has to be sent later
      call.timestamp = System.currentTimeMillis();
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="VO_VOLATILE_INCREMENT",
      justification="False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
  private class Connection extends RpcServer.Connection {
    protected SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    protected final ConcurrentLinkedDeque<Call> responseQueue = new ConcurrentLinkedDeque<Call>();
    private final Lock responseWriteLock = new ReentrantLock();
    private Counter rpcCount = new Counter(); // number of outstanding rpcs
    private long lastContact;
    protected Socket socket;

    private final Call authFailedCall =
      new Call(AUTHORIZATION_FAILED_CALLID, null, null, null, null, null, this, null, 0, null,
        null);

    private final Call saslCall =
      new Call(SASL_CALLID, this.service, null, null, null, null, this, null, 0, null, null);

    public Connection(SocketChannel channel, long lastContact) {
      super();
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      this.addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount.get() == 0;
    }

    /* Decrement the outstanding RPC count */
    protected void decRpcCount() {
      rpcCount.decrement();
    }

    /* Increment the outstanding RPC count */
    protected void incRpcCount() {
      rpcCount.increment();
    }

    protected boolean timedOut(long currentTime) {
      return isIdle() && currentTime - lastContact > maxIdleTime;
    }

    private void saslReadAndProcess(ByteBuffer saslToken) throws IOException,
        InterruptedException {
      if (saslContextEstablished) {
        if (LOG.isTraceEnabled())
          LOG.trace("Have read input token of size " + saslToken.limit()
              + " for processing by saslServer.unwrap()");

        if (!useWrap) {
          processOneRpc(saslToken);
        } else {
          byte[] b = saslToken.array();
          byte [] plaintextData = saslServer.unwrap(b, saslToken.position(), saslToken.limit());
          processUnwrappedData(plaintextData);
        }
      } else {
        byte[] replyToken;
        try {
          if (saslServer == null) {
            switch (authMethod) {
            case DIGEST:
              if (secretManager == null) {
                throw new AccessDeniedException(
                    "Server is not configured to do DIGEST authentication.");
              }
              saslServer = Sasl.createSaslServer(AuthMethod.DIGEST
                  .getMechanismName(), null, SaslUtil.SASL_DEFAULT_REALM,
                  SaslUtil.SASL_PROPS, new SaslDigestCallbackHandler(
                      secretManager, this));
              break;
            default:
              UserGroupInformation current = UserGroupInformation.getCurrentUser();
              String fullName = current.getUserName();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Kerberos principal name is " + fullName);
              }
              final String names[] = SaslUtil.splitKerberosName(fullName);
              if (names.length != 3) {
                throw new AccessDeniedException(
                    "Kerberos principal name does NOT have the expected "
                        + "hostname part: " + fullName);
              }
              current.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws SaslException {
                  saslServer = Sasl.createSaslServer(AuthMethod.KERBEROS
                      .getMechanismName(), names[0], names[1],
                      SaslUtil.SASL_PROPS, new SaslGssCallbackHandler());
                  return null;
                }
              });
            }
            if (saslServer == null)
              throw new AccessDeniedException(
                  "Unable to find SASL server implementation for "
                      + authMethod.getMechanismName());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Created SASL server with mechanism = " + authMethod.getMechanismName());
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Have read input token of size " + saslToken.limit()
                + " for processing by saslServer.evaluateResponse()");
          }
          replyToken = saslServer.evaluateResponse(saslToken.array());
        } catch (IOException e) {
          IOException sendToClient = e;
          Throwable cause = e;
          while (cause != null) {
            if (cause instanceof InvalidToken) {
              sendToClient = (InvalidToken) cause;
              break;
            }
            cause = cause.getCause();
          }
          doRawSaslReply(SaslStatus.ERROR, null, sendToClient.getClass().getName(),
            sendToClient.getLocalizedMessage());
          metrics.authenticationFailure();
          String clientIP = this.toString();
          // attempting user could be null
          AUDITLOG.warn(AUTH_FAILED_FOR + clientIP + ":" + attemptingUser);
          throw e;
        }
        if (replyToken != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Will send token of size " + replyToken.length
                + " from saslServer.");
          }
          doRawSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null,
              null);
        }
        if (saslServer.isComplete()) {
          String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
          useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
          user = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Authenticated client: "
              + user + ". Negotiated QoP is "
              + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          metrics.authenticationSuccess();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
          saslContextEstablished = true;
        }
      }
    }

    /**
     * No protobuf encoding of raw sasl messages
     */
    private void doRawSaslReply(SaslStatus status, Writable rv,
        String errorClass, String error) throws IOException {
      ByteBufferOutputStream saslResponse = null;
      DataOutputStream out = null;
      try {
        // In my testing, have noticed that sasl messages are usually
        // in the ballpark of 100-200. That's why the initial capacity is 256.
        saslResponse = new ByteBufferOutputStream(256);
        out = new DataOutputStream(saslResponse);
        out.writeInt(status.state); // write status
        if (status == SaslStatus.SUCCESS) {
          rv.write(out);
        } else {
          WritableUtils.writeString(out, errorClass);
          WritableUtils.writeString(out, error);
        }
        saslCall.setSaslTokenResponse(saslResponse.getByteBuffer());
        saslCall.responder = responder;
        saslCall.sendResponseIfReady();
      } finally {
        if (saslResponse != null) {
          saslResponse.close();
        }
        if (out != null) {
          out.close();
        }
      }
    }

    private int readPreamble() throws IOException {
      int count;
      // Check for 'HBas' magic.
      this.dataLengthBuffer.flip();
      if (!Arrays.equals(HConstants.RPC_HEADER, dataLengthBuffer.array())) {
        return doBadPreambleHandling("Expected HEADER=" +
            Bytes.toStringBinary(HConstants.RPC_HEADER) +
            " but received HEADER=" + Bytes.toStringBinary(dataLengthBuffer.array()) +
            " from " + toString());
      }
      // Now read the next two bytes, the version and the auth to use.
      ByteBuffer versionAndAuthBytes = ByteBuffer.allocate(2);
      count = channelRead(channel, versionAndAuthBytes);
      if (count < 0 || versionAndAuthBytes.remaining() > 0) {
        return count;
      }
      int version = versionAndAuthBytes.get(0);
      byte authbyte = versionAndAuthBytes.get(1);
      this.authMethod = AuthMethod.valueOf(authbyte);
      if (version != CURRENT_VERSION) {
        String msg = getFatalConnectionString(version, authbyte);
        return doBadPreambleHandling(msg, new WrongVersionException(msg));
      }
      if (authMethod == null) {
        String msg = getFatalConnectionString(version, authbyte);
        return doBadPreambleHandling(msg, new BadAuthException(msg));
      }
      if (isSecurityEnabled && authMethod == AuthMethod.SIMPLE) {
        AccessDeniedException ae = new AccessDeniedException("Authentication is required");
        setupResponse(authFailedResponse, authFailedCall, ae, ae.getMessage());
        responder.doRespond(authFailedCall);
        throw ae;
      }
      if (!isSecurityEnabled && authMethod != AuthMethod.SIMPLE) {
        doRawSaslReply(SaslStatus.SUCCESS, new IntWritable(
            SaslUtil.SWITCH_TO_SIMPLE_AUTH), null, null);
        authMethod = AuthMethod.SIMPLE;
        // client has already sent the initial Sasl message and we
        // should ignore it. Both client and server should fall back
        // to simple auth from now on.
        skipInitialSaslHandshake = true;
      }
      if (authMethod != AuthMethod.SIMPLE) {
        useSasl = true;
      }

      dataLengthBuffer.clear();
      connectionPreambleRead = true;
      return count;
    }

    private int read4Bytes() throws IOException {
      if (this.dataLengthBuffer.remaining() > 0) {
        return channelRead(channel, this.dataLengthBuffer);
      } else {
        return 0;
      }
    }

    /**
     * Read off the wire. If there is not enough data to read, update the connection state with
     *  what we have and returns.
     * @return Returns -1 if failure (and caller will close connection), else zero or more.
     * @throws IOException
     * @throws InterruptedException
     */
    public int readAndProcess() throws IOException, InterruptedException {
      // Try and read in an int.  If new connection, the int will hold the 'HBas' HEADER.  If it
      // does, read in the rest of the connection preamble, the version and the auth method.
      // Else it will be length of the data to read (or -1 if a ping).  We catch the integer
      // length into the 4-byte this.dataLengthBuffer.
      int count = read4Bytes();
      if (count < 0 || dataLengthBuffer.remaining() > 0) {
        return count;
      }

      // If we have not read the connection setup preamble, look to see if that is on the wire.
      if (!connectionPreambleRead) {
        count = readPreamble();
        if (!connectionPreambleRead) {
          return count;
        }

        count = read4Bytes();
        if (count < 0 || dataLengthBuffer.remaining() > 0) {
          return count;
        }
      }

      // We have read a length and we have read the preamble.  It is either the connection header
      // or it is a request.
      if (data == null) {
        dataLengthBuffer.flip();
        int dataLength = dataLengthBuffer.getInt();
        if (dataLength == RpcClient.PING_CALL_ID) {
          if (!useWrap) { //covers the !useSasl too
            dataLengthBuffer.clear();
            return 0;  //ping message
          }
        }
        if (dataLength < 0) { // A data length of zero is legal.
          throw new IllegalArgumentException("Unexpected data length "
              + dataLength + "!! from " + getHostAddress());
        }
        if (dataLength > maxRpcSize) {
          String warningMsg =
              "data length is too large: " + dataLength + "!! from " + getHostAddress() + ":"
                  + getRemotePort();
          LOG.warn(warningMsg);
          throw new DoNotRetryIOException(warningMsg);
        }

       // TODO: check dataLength against some limit so that the client cannot OOM the server
        data = ByteBuffer.allocate(dataLength);

        // Increment the rpc count. This counter will be decreased when we write
        //  the response.  If we want the connection to be detected as idle properly, we
        //  need to keep the inc / dec correct.
        incRpcCount();
      }

      count = channelRead(channel, data);

      if (count >= 0 && data.remaining() == 0) { // count==0 if dataLength == 0
        process();
      }

      return count;
    }

    /**
     * Process the data buffer and clean the connection state for the next call.
     */
    private void process() throws IOException, InterruptedException {
      data.flip();
      try {
        if (skipInitialSaslHandshake) {
          skipInitialSaslHandshake = false;
          return;
        }

        if (useSasl) {
          saslReadAndProcess(data);
        } else {
          processOneRpc(data);
        }

      } finally {
        dataLengthBuffer.clear(); // Clean for the next call
        data = null; // For the GC
      }
    }

    private int doBadPreambleHandling(final String msg) throws IOException {
      return doBadPreambleHandling(msg, new FatalConnectionException(msg));
    }

    private int doBadPreambleHandling(final String msg, final Exception e) throws IOException {
      LOG.warn(msg);
      Call fakeCall = new Call(-1, null, null, null, null, null, this, responder, -1, null, null);
      setupResponse(null, fakeCall, e, msg);
      responder.doRespond(fakeCall);
      // Returning -1 closes out the connection.
      return -1;
    }

    private void processUnwrappedData(byte[] inBuf) throws IOException,
    InterruptedException {
      ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        int count;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

          if (unwrappedDataLength == RpcClient.PING_CALL_ID) {
            if (LOG.isDebugEnabled())
              LOG.debug("Received ping message");
            unwrappedDataLengthBuffer.clear();
            continue; // ping message
          }
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          processOneRpc(unwrappedData);
          unwrappedData = null;
        }
      }
    }


    private void processOneRpc(ByteBuffer buf) throws IOException, InterruptedException {
      if (connectionHeaderRead) {
        processRequest(buf);
      } else {
        processConnectionHeader(buf);
        this.connectionHeaderRead = true;
        if (!authorizeConnection()) {
          // Throw FatalConnectionException wrapping ACE so client does right thing and closes
          // down the connection instead of trying to read non-existent retun.
          throw new AccessDeniedException("Connection from " + this + " for service " +
            connectionHeader.getServiceName() + " is unauthorized for user: " + user);
        }
      }
    }

    /**
     * @param buf Has the request header and the request param and optionally encoded data buffer
     * all in this one array.
     * @throws IOException
     * @throws InterruptedException
     */
    protected void processRequest(ByteBuffer buf) throws IOException, InterruptedException {
      long totalRequestSize = buf.limit();
      int offset = 0;
      // Here we read in the header.  We avoid having pb
      // do its default 4k allocation for CodedInputStream.  We force it to use backing array.
      CodedInputStream cis = CodedInputStream.newInstance(buf.array(), offset, buf.limit());
      int headerSize = cis.readRawVarint32();
      offset = cis.getTotalBytesRead();
      Message.Builder builder = RequestHeader.newBuilder();
      ProtobufUtil.mergeFrom(builder, cis, headerSize);
      RequestHeader header = (RequestHeader) builder.build();
      offset += headerSize;
      int id = header.getCallId();
      if (LOG.isTraceEnabled()) {
        LOG.trace("RequestHeader " + TextFormat.shortDebugString(header) +
          " totalRequestSize: " + totalRequestSize + " bytes");
      }
      // Enforcing the call queue size, this triggers a retry in the client
      // This is a bit late to be doing this check - we have already read in the total request.
      if ((totalRequestSize + callQueueSize.get()) > maxQueueSize) {
        final Call callTooBig =
          new Call(id, this.service, null, null, null, null, this,
            responder, totalRequestSize, null, null);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, callTooBig, CALL_QUEUE_TOO_BIG_EXCEPTION,
            "Call queue is full on " + getListenerAddress() +
                ", is hbase.ipc.server.max.callqueue.size too small?");
        responder.doRespond(callTooBig);
        return;
      }
      MethodDescriptor md = null;
      Message param = null;
      CellScanner cellScanner = null;
      try {
        if (header.hasRequestParam() && header.getRequestParam()) {
          md = this.service.getDescriptorForType().findMethodByName(header.getMethodName());
          if (md == null) throw new UnsupportedOperationException(header.getMethodName());
          builder = this.service.getRequestPrototype(md).newBuilderForType();
          cis.resetSizeCounter();
          int paramSize = cis.readRawVarint32();
          offset += cis.getTotalBytesRead();
          if (builder != null) {
            ProtobufUtil.mergeFrom(builder, cis, paramSize);
            param = builder.build();
          }
          offset += paramSize;
        } else {
          // currently header must have request param, so we directly throw exception here
          String msg = "Invalid request header: {" + TextFormat.shortDebugString(header)
              + "}, should have param set in it";
          LOG.warn(msg);
          throw new DoNotRetryIOException(msg);
        }
        if (header.hasCellBlockMeta()) {
          buf.position(offset);
          cellScanner = ipcUtil.createCellScanner(this.codec, this.compressionCodec, buf);
        }
      } catch (Throwable t) {
        String msg = getListenerAddress() + " is unable to read call parameter from client " +
            getHostAddress();
        LOG.warn(msg, t);

        metrics.exception(t);

        // probably the hbase hadoop version does not match the running hadoop version
        if (t instanceof LinkageError) {
          t = new DoNotRetryIOException(t);
        }
        // If the method is not present on the server, do not retry.
        if (t instanceof UnsupportedOperationException) {
          t = new DoNotRetryIOException(t);
        }

        final Call readParamsFailedCall =
          new Call(id, this.service, null, null, null, null, this,
            responder, totalRequestSize, null, null);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        setupResponse(responseBuffer, readParamsFailedCall, t,
          msg + "; " + t.getMessage());
        responder.doRespond(readParamsFailedCall);
        return;
      }

      TraceInfo traceInfo = header.hasTraceInfo()
          ? new TraceInfo(header.getTraceInfo().getTraceId(), header.getTraceInfo().getParentId())
          : null;
      Call call = new Call(id, this.service, md, header, param, cellScanner, this, responder,
              totalRequestSize, traceInfo, this.addr);

      if (!scheduler.dispatch(new CallRunner(SimpleRpcServer.this, call))) {
        callQueueSize.add(-1 * call.getSize());

        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        InetSocketAddress address = getListenerAddress();
        setupResponse(responseBuffer, call, CALL_QUEUE_TOO_BIG_EXCEPTION,
            "Call queue is full on " + (address != null ? address : "(channel closed)") +
                ", too many items queued ?");
        responder.doRespond(call);
      }
    }

    private boolean authorizeConnection() throws IOException {
      try {
        // If auth method is DIGEST, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (user != null && user.getRealUser() != null
            && (authMethod != AuthMethod.DIGEST)) {
          ProxyUsers.authorize(user, this.getHostAddress(), conf);
        }
        authorize(user, connectionHeader, getHostInetAddress());
        metrics.authorizationSuccess();
      } catch (AuthorizationException ae) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connection authorization failed: " + ae.getMessage(), ae);
        }
        metrics.authorizationFailure();
        setupResponse(authFailedResponse, authFailedCall,
          new AccessDeniedException(ae), ae.getMessage());
        responder.doRespond(authFailedCall);
        return false;
      }
      return true;
    }

    protected synchronized void close() {
      disposeSasl();
      data = null;
      if (!channel.isOpen()) {
        cleanupResponseQueue();
        return;
      }
      try {socket.shutdownOutput();} catch(Exception ignored) {} // FindBugs DE_MIGHT_IGNORE
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception ignored) {}
      }
      try {socket.close();} catch(Exception ignored) {}
      cleanupResponseQueue();
    }

    private void cleanupResponseQueue() {
      // Decrease response queue size and do necessary logging when closing connection
      Call call;
      while ((call = this.responseQueue.poll()) != null) {
        try {
          LOG.warn("Response discarded for call {" + call.toString()
              + "} because connection closed");
        } catch (Throwable t) {
          LOG.debug("Exception occurred during logging", t);
        }
        responseQueueSize.add(-1L * call.response.getRemaining());
        responseQueueLength.decrementAndGet();
      }
    }

    @Override
    public boolean isConnectionOpen() {
      return channel.isOpen();
    }
  }

  /**
   * Constructs a server listening on the named port and address.
   * @param server hosting instance of {@link Server}. We will do authentications if an
   * instance else pass null for no authentication check.
   * @param name Used keying this rpc servers' metrics and for naming the Listener thread.
   * @param services A list of services.
   * @param bindAddress Where to listen
   * @param conf
   * @param scheduler
   */
  public SimpleRpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress bindAddress, Configuration conf,
      RpcScheduler scheduler)
      throws IOException {
    super(server, name, services, bindAddress, conf, scheduler);
    this.bindAddress = bindAddress;
    this.socketSendBufferSize = 0;
    this.readThreads = conf.getInt("hbase.ipc.server.read.threadpool.size", 10);
    this.maxIdleTime = 2 * conf.getInt("hbase.ipc.client.connection.maxidletime", 1000);
    this.maxConnectionsToNuke = conf.getInt("hbase.ipc.client.kill.max", 10);
    this.thresholdIdleConnections = conf.getInt("hbase.ipc.client.idlethreshold", 4000);
    this.purgeTimeout = conf.getLong("hbase.ipc.client.call.purge.timeout",
      2 * HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

    // Start the listener here and let it bind to the port
    listener = new Listener(name);
    this.port = listener.getAddress().getPort();

    this.tcpNoDelay = conf.getBoolean("hbase.ipc.server.tcpnodelay", true);
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.server.tcpkeepalive", true);

    this.warnDelayedCalls = conf.getInt(WARN_DELAYED_CALLS, DEFAULT_WARN_DELAYED_CALLS);
    this.delayedCalls = new AtomicInteger(0);

    // Create the responder here
    responder = new Responder();
    this.scheduler.init(new RpcSchedulerContext(this));
  }

  /**
   * Subclasses of HBaseServer can override this to provide their own
   * Connection implementations.
   */
  protected Connection getConnection(SocketChannel channel, long time) {
    return new Connection(channel, time);
  }

  /**
   * Setup response for the RPC Call.
   *
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(ByteArrayOutputStream response, Call call, Throwable t, String error)
  throws IOException {
    if (response != null) response.reset();
    call.setResponse(null, null, t, error);
  }

  protected void closeConnection(Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection)) {
        numConnections--;
      }
    }
    connection.close();
  }

  /** Sets the socket buffer size used for responding to RPCs.
   * @param size send size
   */
  @Override
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  /** Starts the service.  Must be called before any calls will be handled. */
  @Override
  public synchronized void start() {
    if (started) return;
    authTokenSecretMgr = createSecretManager();
    if (authTokenSecretMgr != null) {
      setSecretManager(authTokenSecretMgr);
      authTokenSecretMgr.start();
    }
    this.authManager = new ServiceAuthorizationManager();
    HBasePolicyProvider.init(conf, authManager);
    responder.start();
    listener.start();
    scheduler.start();
    started = true;
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  @Override
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (authTokenSecretMgr != null) {
      authTokenSecretMgr.stop();
      authTokenSecretMgr = null;
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    scheduler.stop();
    notifyAll();
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   * @throws InterruptedException e
   */
  @Override
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  @Override
  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }

  /**
   * This is a wrapper around {@link java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large
   * buffer.
   *
   * @param channel writable byte channel to write to
   * @param bufferChain Chain of buffers to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)
   */
  protected long channelWrite(GatheringByteChannel channel, BufferChain bufferChain)
  throws IOException {
    long count =  bufferChain.write(channel, NIO_BUFFER_LIMIT);
    if (count > 0) this.metrics.sentBytes(count);
    return count;
  }

  /**
   * This is a wrapper around {@link java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * ByteBuffer increases. There should not be any performance degredation.
   *
   * @param channel writable byte channel to write on
   * @param buffer buffer to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
   */
  protected int channelRead(ReadableByteChannel channel,
                                   ByteBuffer buffer) throws IOException {

    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      metrics.receivedBytes(count);
    }
    return count;
  }

  /**
   * A convenience method to bind to a given address and report
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address,
                          int backlog) throws IOException {
    try {
      socket.bind(address, backlog);
    } catch (BindException e) {
      BindException bindException =
        new BindException("Problem binding to " + address + " : " +
            e.getMessage());
      bindException.initCause(e);
      throw bindException;
    } catch (SocketException e) {
      // If they try to bind to a different host's address, give a better
      // error message.
      if ("Unresolved address".equals(e.getMessage())) {
        throw new UnknownHostException("Invalid hostname for server: " +
                                       address.getHostName());
      }
      throw e;
    }
  }

  @Override
  public int getNumOpenConnections() {
    return connectionList.size();
  }
  
  @Override
  public long getResponseQueueSize() {
    return responseQueueSize.get();
  }

  @Override
  public int getResponseQueueLength() {
    return responseQueueLength.get();
  }

  @Override
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
      Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
      throws IOException {
    Call fakeCall = new Call(-1, service, md, null, param, cellScanner, null, null, -1, null, null);
    fakeCall.setReceiveTime(receiveTime);
    return call(fakeCall, status);
  }
}
