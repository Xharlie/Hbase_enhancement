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
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslDigestCallbackHandler;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.htrace.TraceInfo;

public class NettyRpcServer extends RpcServer {

  static {
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
  }

  protected final InetSocketAddress bindAddress;

  private final Channel serverChannel;
  private final ChannelGroup allChannels = new DefaultChannelGroup("hbase-rpc-netty-server");
  private final ChannelFactory channelFactory;
  private final CountDownLatch closed = new CountDownLatch(1);
  private AtomicBoolean releaseNettyResources = new AtomicBoolean(true);

  class ExceptionHandler implements UncaughtExceptionHandler {

    AtomicBoolean releaseNettyResources;
    Server server;

    ExceptionHandler(AtomicBoolean releaseNettyResources, Server server) {
      this.releaseNettyResources = releaseNettyResources;
      this.server = server;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      releaseNettyResources.set(false);
      LOG.error(t.toString() + " throw uncaught exception", e);
      LOG.info("Trigger server abort since we cannot serve requests anymore");
      server.abort(t.toString() + " throw uncaught exception", e);
      // Runtime.getRuntime().halt(1);
    }

  }

  public NettyRpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services, final InetSocketAddress bindAddress,
      Configuration conf, RpcScheduler scheduler) {
    super(server, name, services, bindAddress, conf, scheduler);
    this.bindAddress = bindAddress;

    Thread.setDefaultUncaughtExceptionHandler(new ExceptionHandler(releaseNettyResources, server));
    int workerCount = conf.getInt("hbase.rpc.server.netty.work.size", 12);
    LOG.info("hbase.rpc.server.netty.work.size: " + workerCount);
    this.channelFactory =
        new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), new NioWorkerPool(
            Executors.newCachedThreadPool(), workerCount));

    ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        p.addLast("frameDecoder", new MessageDecoder());
        p.addLast("frameEncoder", new MessageEncoder());
        return p;
      }
    });
    serverChannel = bootstrap.bind(this.bindAddress);
    LOG.info("NettyRpcServer bind to: " + serverChannel.getLocalAddress());
    allChannels.add(serverChannel);

    this.scheduler.init(new RpcSchedulerContext(this));
  }

  @Override
  public void start() {
    if (started) {
      return;
    }
    LOG.info("Start server on " + this.bindAddress.getPort());
    authTokenSecretMgr = createSecretManager();
    if (authTokenSecretMgr != null) {
      setSecretManager(authTokenSecretMgr);
      authTokenSecretMgr.start();
    }
    this.authManager = new ServiceAuthorizationManager();
    HBasePolicyProvider.init(conf, authManager);
    scheduler.start();
    started = true;
  }

  @Override
  public void stop() {
    LOG.info("Stopping server on " + this.bindAddress.getPort());
    if (authTokenSecretMgr != null) {
      authTokenSecretMgr.stop();
      authTokenSecretMgr = null;
    }
    if (releaseNettyResources.get()) {
      ChannelGroupFuture future = allChannels.close();
      future.awaitUninterruptibly();
      channelFactory.releaseExternalResources();
    }
    scheduler.stop();
    closed.countDown();
  }

  @Override
  public void join() throws InterruptedException {
    closed.await();
  }

  @Override
  public InetSocketAddress getListenerAddress() {
    return ((InetSocketAddress) serverChannel.getLocalAddress());
  }

  private void setupResponse(ByteArrayOutputStream response, Call call, Throwable t, String error)
      throws IOException {
    if (response != null) response.reset();
    call.setResponse(null, null, t, error);
  }

  class Connection extends RpcServer.Connection {

    private final Call authFailedCall = new Call(AUTHORIZATION_FAILED_CALLID, null, null, null,
        null, null, this, 0, null, null);

    private final Call saslCall = new Call(SASL_CALLID, this.service, null, null, null, null, this,
        0, null, null);

    protected Channel channel;

    Connection(Channel channel) {
      super();
      this.channel = channel;
      InetSocketAddress inetSocketAddress = ((InetSocketAddress) channel.getRemoteAddress());
      this.addr = inetSocketAddress.getAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = inetSocketAddress.getAddress().getHostAddress();
      }
      this.remotePort = inetSocketAddress.getPort();
    }

    void readPreamble(ChannelBuffer buffer) throws IOException {
      byte[] rpcHead =
          { buffer.readByte(), buffer.readByte(), buffer.readByte(), buffer.readByte() };
      if (!Arrays.equals(HConstants.RPC_HEADER, rpcHead)) {
        doBadPreambleHandling("Expected HEADER=" + Bytes.toStringBinary(HConstants.RPC_HEADER)
            + " but received HEADER=" + Bytes.toStringBinary(rpcHead) + " from " + toString());
        return;
      }
      // Now read the next two bytes, the version and the auth to use.
      int version = buffer.readByte();
      byte authbyte = buffer.readByte();
      this.authMethod = AuthMethod.valueOf(authbyte);
      if (version != CURRENT_VERSION) {
        String msg = getFatalConnectionString(version, authbyte);
        doBadPreambleHandling(msg, new WrongVersionException(msg));
        return;
      }
      if (authMethod == null) {
        String msg = getFatalConnectionString(version, authbyte);
        doBadPreambleHandling(msg, new BadAuthException(msg));
        return;
      }
      if (isSecurityEnabled && authMethod == AuthMethod.SIMPLE) {
        AccessDeniedException ae = new AccessDeniedException("Authentication is required");
        setupResponse(authFailedResponse, authFailedCall, ae, ae.getMessage());
        authFailedCall.sendResponseIfReady(ChannelFutureListener.CLOSE);
        return;
      }
      if (!isSecurityEnabled && authMethod != AuthMethod.SIMPLE) {
        doRawSaslReply(SaslStatus.SUCCESS, new IntWritable(SaslUtil.SWITCH_TO_SIMPLE_AUTH), null,
          null);
        authMethod = AuthMethod.SIMPLE;
        // client has already sent the initial Sasl message and we
        // should ignore it. Both client and server should fall back
        // to simple auth from now on.
        skipInitialSaslHandshake = true;
      }
      if (authMethod != AuthMethod.SIMPLE) {
        useSasl = true;
      }
    }

    private void doRawSaslReply(SaslStatus status, Writable rv, String errorClass, String error)
        throws IOException {
      ByteBufferOutputStream saslResponse = null;
      DataOutputStream out = null;
      try {
        // In my testing, have noticed that sasl messages are usually
        // in the ballpark of 100-200. That's why the initial capacity
        // is 256.
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
        //saslCall.responder = responder;
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

    private void doBadPreambleHandling(final String msg) throws IOException {
      doBadPreambleHandling(msg, new FatalConnectionException(msg));
    }

    private void doBadPreambleHandling(final String msg, final Exception e) throws IOException {
      LOG.warn(msg);
      Call fakeCall = new Call(-1, null, null, null, null, null, this, -1, null, null);
      setupResponse(null, fakeCall, e, msg);
      // closes out the connection.
      fakeCall.sendResponseIfReady(ChannelFutureListener.CLOSE);
    }

    void process(ByteBuffer buf) throws IOException, InterruptedException {
      if (skipInitialSaslHandshake) {
        skipInitialSaslHandshake = false;
        return;
      }

      if (useSasl) {
        saslReadAndProcess(buf);
      } else {
        processOneRpc(buf);
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
          throw new AccessDeniedException("Connection from " + this + " for service "
              + connectionHeader.getServiceName() + " is unauthorized for user: " + user);
        }
      }
    }

    void processRequest(ByteBuffer buf) throws IOException, InterruptedException {
      long totalRequestSize = buf.remaining();
      int offset = buf.position();
      // Here we read in the header. We avoid having pb
      // do its default 4k allocation for CodedInputStream. We force it to
      // use backing array.
      CodedInputStream cis =
          CodedInputStream.newInstance(buf.array(), buf.arrayOffset() + offset, buf.remaining());
      int headerSize = cis.readRawVarint32();
      offset += cis.getTotalBytesRead();
      Message.Builder builder = RequestHeader.newBuilder();
      ProtobufUtil.mergeFrom(builder, cis, headerSize);
      RequestHeader header = (RequestHeader) builder.build();
      offset += headerSize;
      int id = header.getCallId();
      if (LOG.isTraceEnabled()) {
        LOG.trace("RequestHeader " + TextFormat.shortDebugString(header) + " totalRequestSize: "
            + totalRequestSize + " bytes");
      }
      // Enforcing the call queue size, this triggers a retry in the
      // client
      // This is a bit late to be doing this check - we have already read
      // in the total request.
      if ((totalRequestSize + callQueueSize.get()) > maxQueueSize) {
        final Call callTooBig =
            new Call(id, this.service, null, null, null, null, this, totalRequestSize, null, null);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, callTooBig, CALL_QUEUE_TOO_BIG_EXCEPTION,
          "Call queue is full on " + getListenerAddress()
              + ", is hbase.ipc.server.max.callqueue.size too small?");
        callTooBig.sendResponseIfReady();
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
        }
        if (header.hasCellBlockMeta()) {
          buf.position(offset);
          cellScanner = ipcUtil.createCellScanner(this.codec, this.compressionCodec, buf);
        }
      } catch (Throwable t) {
        String msg =
            getListenerAddress() + " is unable to read call parameter from client "
                + getHostAddress();
        LOG.warn(msg, t);

        metrics.exception(t);

        // probably the hbase hadoop version does not match the running
        // hadoop version
        if (t instanceof LinkageError) {
          t = new DoNotRetryIOException(t);
        }
        // If the method is not present on the server, do not retry.
        if (t instanceof UnsupportedOperationException) {
          t = new DoNotRetryIOException(t);
        }

        final Call readParamsFailedCall =
            new Call(id, this.service, null, null, null, null, this, totalRequestSize, null, null);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        setupResponse(responseBuffer, readParamsFailedCall, t, msg + "; " + t.getMessage());
        readParamsFailedCall.sendResponseIfReady();
        return;
      }

      TraceInfo traceInfo =
          header.hasTraceInfo() ? new TraceInfo(header.getTraceInfo().getTraceId(), header
              .getTraceInfo().getParentId()) : null;
      Call call =
          new Call(id, this.service, md, header, param, cellScanner, this, totalRequestSize,
              traceInfo, this.addr);
      if (!scheduler.dispatch(new CallRunner(NettyRpcServer.this, call))) {
        callQueueSize.add(-1 * call.getSize());

        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        InetSocketAddress address = getListenerAddress();
        setupResponse(responseBuffer, call, CALL_QUEUE_TOO_BIG_EXCEPTION, "Call queue is full on "
            + (address != null ? address : "(channel closed)") + ", too many items queued ?");
        call.sendResponseIfReady();
      }
    }

    private void saslReadAndProcess(ByteBuffer saslToken) throws IOException, InterruptedException {
      if (saslContextEstablished) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Have read input token of size " + saslToken.remaining()
              + " for processing by saslServer.unwrap()");
        }

        if (!useWrap) {
          processOneRpc(saslToken);
        } else {
          byte[] b = saslToken.array();
          byte[] plaintextData =
              saslServer.unwrap(b, saslToken.arrayOffset() + saslToken.position(),
                saslToken.limit());
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
              saslServer =
                  Sasl.createSaslServer(AuthMethod.DIGEST.getMechanismName(), null,
                    SaslUtil.SASL_DEFAULT_REALM, SaslUtil.SASL_PROPS,
                    new SaslDigestCallbackHandler(secretManager, this));
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
                    "Kerberos principal name does NOT have the expected " + "hostname part: "
                        + fullName);
              }
              current.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws SaslException {
                  saslServer =
                      Sasl.createSaslServer(AuthMethod.KERBEROS.getMechanismName(), names[0],
                        names[1], SaslUtil.SASL_PROPS, new SaslGssCallbackHandler());
                  return null;
                }
              });
            }
            if (saslServer == null) {
              throw new AccessDeniedException("Unable to find SASL server implementation for "
                  + authMethod.getMechanismName());
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Created SASL server with mechanism = " + authMethod.getMechanismName());
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Have read input token of size " + saslToken.remaining()
                + " for processing by saslServer.evaluateResponse()");
          }
          byte[] data = new byte[saslToken.remaining()];
          saslToken.get(data);
          replyToken = saslServer.evaluateResponse(data);
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
            LOG.debug("Will send token of size " + replyToken.length + " from saslServer.");
          }
          doRawSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null, null);
        }
        if (saslServer.isComplete()) {
          String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
          useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
          user = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Authenticated client: " + user
                + ". Negotiated QoP is " + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          metrics.authenticationSuccess();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
          saslContextEstablished = true;
        }
      }
    }

    private void processUnwrappedData(byte[] inBuf) throws IOException, InterruptedException {
      ReadableByteChannel ch =
          java.nio.channels.Channels.newChannel(new ByteArrayInputStream(inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        int count;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0) return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

          if (unwrappedDataLength == RpcClient.PING_CALL_ID) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Received ping message");
            }
            unwrappedDataLengthBuffer.clear();
            continue; // ping message
          }
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0) return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          processOneRpc(unwrappedData);
          unwrappedData = null;
        }
      }
    }

    protected int channelRead(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
      int count =
          (buffer.remaining() <= NIO_BUFFER_LIMIT) ? channel.read(buffer) : channelIO(channel,
            null, buffer);
      return count;
    }

    private boolean authorizeConnection() throws IOException {
      try {
        // If auth method is DIGEST, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (user != null && user.getRealUser() != null && (authMethod != AuthMethod.DIGEST)) {
          ProxyUsers.authorize(user, this.getHostAddress(), conf);
        }
        authorize(user, connectionHeader, getHostInetAddress());
        metrics.authorizationSuccess();
      } catch (AuthorizationException ae) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connection authorization failed: " + ae.getMessage(), ae);
        }
        metrics.authorizationFailure();
        setupResponse(authFailedResponse, authFailedCall, new AccessDeniedException(ae),
          ae.getMessage());
        // responder.doRespond(authFailedCall);
        return false;
      }
      return true;
    }

    public void close() {
      disposeSasl();
    }

    @Override
    public boolean isConnectionOpen() {
      return channel.isOpen();
    }
  }

  /**
   * Datastructure that holds all necessary to a method invocation and then afterward, carries the
   * result.
   */
  public class Call extends RpcServer.Call {

    // set at call completion

    Call(int id, final BlockingService service, final MethodDescriptor md, RequestHeader header,
        Message param, CellScanner cellScanner, Connection connection, long size, TraceInfo tinfo,
        final InetAddress remoteAddress) {
      super(id, service, md, header, param, cellScanner, connection, size, tinfo, remoteAddress);
    }

    @Override
    public synchronized void endDelay(Object result) throws IOException {
      throw new IOException("Not support delay response");
    }

    @Override
    public synchronized void endDelay() throws IOException {
      this.endDelay(null);
    }

    @Override
    public synchronized void startDelay(boolean delayReturnValue) {
    }

    @Override
    public synchronized void endDelayThrowing(Throwable t) throws IOException {
      this.setResponse(null, null, t, StringUtils.stringifyException(t));
      this.sendResponseIfReady();
    }

    @Override
    public synchronized boolean isDelayed() {
      return false;
    }

    @Override
    public synchronized boolean isReturnValueDelayed() {
      return false;
    }

    Connection getConnection() {
      return (Connection) this.connection;
    }

    /**
     * If we have a response, and delay is not set, then respond immediately. Otherwise, do not
     * respond to client. This is called by the RPC code in the context of the Handler thread.
     */
    public synchronized void sendResponseIfReady() throws IOException {
      getConnection().channel.write(response).addListener(new CallWriteListener(this));
    }

    public synchronized void sendResponseIfReady(ChannelFutureListener listener) throws IOException {
      getConnection().channel.write(response).addListener(listener);
    }

  }

  class MessageDecoder extends FrameDecoder {
    // If initial preamble with version and magic has been read or not.
    private boolean connectionPreambleRead = false;

    private ChannelBuffer cumulation;
    private Connection connection;

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      allChannels.add(e.getChannel());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connection from " + e.getChannel().getRemoteAddress()
            + "; # active connections: " + getNumOpenConnections());
      }
      super.channelOpen(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      Object m = e.getMessage();
      if (!(m instanceof ChannelBuffer)) {
        ctx.sendUpstream(e);
        return;
      }

      ChannelBuffer input = (ChannelBuffer) m;
      if (!input.readable()) {
        return;
      }
      // If we have not read the connection setup preamble, look to see if
      // that is on the wire.
      if (!connectionPreambleRead) {
        readPreamble(ctx, input);
        if (!connectionPreambleRead) {
          return;
        }
      }
      metrics.receivedBytes(input.readableBytes());
      // ChannelBuffer cumulation = cumulation(ctx);
      // if (cumulation.readable()) {
      // cumulation.discardReadBytes();
      // cumulation.writeBytes(input);
      // callDecode(ctx, e.getChannel(), cumulation, e.getRemoteAddress());
      // } else {
      // callDecode(ctx, e.getChannel(), input, e.getRemoteAddress());
      // if (input.readable()) {
      // cumulation.writeBytes(input);
      // }
      // }
      if (cumulation == null) {
        cumulation = input;
      } else {
        ChannelBuffer oldCumulation = cumulation;
        cumulation =
            ChannelBuffers.dynamicBuffer(oldCumulation.readableBytes() + input.readableBytes(), ctx
                .getChannel().getConfig().getBufferFactory());
        cumulation.writeBytes(oldCumulation);
        cumulation.writeBytes(input);
        oldCumulation = null;
      }
      callDecode(ctx, e.getChannel(), cumulation, e.getRemoteAddress());
      if (cumulation != null && !cumulation.readable()) {
        cumulation = null;
      }
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf)
        throws Exception {
      ByteBuffer data = getData(buf);
      if (data != null) {
        connection.process(data);
      }
      return null;
    }

    private void callDecode(ChannelHandlerContext context, Channel channel,
        ChannelBuffer cumulation, SocketAddress remoteAddress) throws Exception {
      // pipeline to receive requests or responses
      List<Object> results = new ArrayList<Object>();
      while (cumulation.readable()) {
        int oldReaderIndex = cumulation.readerIndex();
        Object frame = decode(context, channel, cumulation);
        if (frame == null) {
          if (oldReaderIndex == cumulation.readerIndex()) {
            // Seems like more data is required.
            // Let us wait for the next notification.
            break;
          } else {
            // Previous data has been discarded.
            // Probably it is reading on.
            continue;
          }
        } else if (oldReaderIndex == cumulation.readerIndex()) {
          throw new IllegalStateException("decode() method must read at least one byte "
              + "if it returned a frame (caused by: " + getClass() + ")");
        }

        results.add(frame);
      }
      if (results.size() > 0) {
        fireMessageReceived(context, remoteAddress, results);
      }
    }

    private void readPreamble(ChannelHandlerContext ctx, ChannelBuffer input) throws IOException {
      if (input.readableBytes() < 6) {
        return;
      }
      metrics.receivedBytes(6);
      connection = new Connection(ctx.getChannel());
      connection.readPreamble(input);
      connectionPreambleRead = true;
    }

    private ByteBuffer getData(ChannelBuffer buf) throws Exception {
      // Make sure if the length field was received.
      if (buf.readableBytes() < 4) {
        // The length field was not received yet - return null.
        // This method will be invoked again when more packets are
        // received and appended to the buffer.
        return null;
      }
      // The length field is in the buffer.

      // Mark the current buffer position before reading the length field
      // because the whole frame might not be in the buffer yet.
      // We will reset the buffer position to the marked position if
      // there's not enough bytes in the buffer.
      buf.markReaderIndex();

      // Read the length field.
      int length = buf.readInt();

      if (length == RpcClient.PING_CALL_ID) {
        if (!connection.useWrap) { // covers the !useSasl too
          return null; // ping message
        }
      }
      if (length < 0) { // A data length of zero is legal.
        throw new IllegalArgumentException("Unexpected data length " + length + "!! from "
            + connection.getHostAddress());
      }
      if (length > maxRpcSize) {
        String warningMsg =
            "data length is too large: " + length + "!! from " + connection.getHostAddress() + ":"
                + connection.getRemotePort();
        LOG.warn(warningMsg);
        throw new DoNotRetryIOException(warningMsg);
      }

      // Make sure if there's enough bytes in the buffer.
      if (buf.readableBytes() < length) {
        // The whole bytes were not received yet - return null.
        // This method will be invoked again when more packets are
        // received and appended to the buffer.

        // Reset to the marked position to read the length field again
        // next time.
        buf.resetReaderIndex();
        return null;
      }
      // There's enough bytes in the buffer. Read it.
      ByteBuffer data = buf.toByteBuffer(buf.readerIndex(), length);
      buf.skipBytes(length);
      //ByteBuffer data = ByteBuffer.allocate(length);
      //buf.readBytes(data);
      //data.flip();
      return data;
    }

    private void fireMessageReceived(ChannelHandlerContext context, SocketAddress remoteAddress,
        Object result) {
      Channels.fireMessageReceived(context, result, remoteAddress);
    }

    private ChannelBuffer cumulation(ChannelHandlerContext ctx) {
      ChannelBuffer c = cumulation;
      if (c == null) {
        c = ChannelBuffers.dynamicBuffer(ctx.getChannel().getConfig().getBufferFactory());
        cumulation = c;
      }
      return c;
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Disconnecting client: " + e.getChannel().getRemoteAddress()
            + ". Number of active connections: " + getNumOpenConnections());
      }
      allChannels.remove(e.getChannel());
      if (connection != null) {
        connection.close();
      }
      super.channelClosed(ctx, e);
      e.getChannel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connection from " + e.getChannel().getRemoteAddress()
            + " catch unexpected exception from downstream.", e.getCause());
      }
      allChannels.remove(e.getChannel());
      if (connection != null) {
        connection.close();
      }
      e.getChannel().close();
    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
      metrics.sentBytes(e.getWrittenAmount());
      ctx.sendUpstream(e);
    }

  }

  class MessageEncoder extends OneToOneEncoder {

    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object message)
        throws Exception {
      if (message instanceof BufferChain) {
        BufferChain response = (BufferChain) message;
        ByteBuffer[] buffers = response.getBuffers();
        ChannelBuffer[] data = new ChannelBuffer[buffers.length];
        for (int i = 0; i < data.length; i++) {
          data[i] = ChannelBuffers.wrappedBuffer(buffers[i]);
        }
        // TODO Netty has no parameter like SimpleRpcServer#NIO_BUFFER_LIMIT to control maximum size
        // of a single packet, revisit here if any problem occurs
        return ChannelBuffers.wrappedBuffer(true, data);
      } else {
        LOG.error("encode unknow request object");
        throw new IOException("Unkown message");
      }
    }

  }

  class CallWriteListener implements ChannelFutureListener {
    private Call call;

    CallWriteListener(Call call) {
      this.call = call;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      call.done();
    }

  }

  @Override
  public void setSocketSendBufSize(int size) {
  }

  @Override
  public int getNumOpenConnections() {
    // allChannels also contains the server channel, so exclude that from the count.
    return allChannels.size() - 1;
  }

  @Override
  public long getResponseQueueSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getResponseQueueLength() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
      Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
      throws IOException {
    Call fakeCall = new Call(-1, service, md, null, param, cellScanner, null, -1, null, null);
    fakeCall.setReceiveTime(receiveTime);
    return call(fakeCall, status);
  }
}
