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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.RecyclableArrayList;
import io.netty.util.internal.StringUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

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
import org.apache.hadoop.hbase.util.JVM;
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

public class Netty4RpcServer extends RpcServer {

  protected final InetSocketAddress bindAddress;

  private final CountDownLatch closed = new CountDownLatch(1);
  private final Channel serverChannel;
  private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);;

  public Netty4RpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services, final InetSocketAddress bindAddress,
      Configuration conf, RpcScheduler scheduler) throws InterruptedException {
    super(server, name, services, bindAddress, conf, scheduler);
    this.bindAddress = bindAddress;
    boolean useEpoll = useEpoll(conf);
    LOG.info("useEpoll: " + useEpoll);
    int workerCount = conf.getInt("hbase.rpc.server.netty.work.size", 12);
    LOG.info("hbase.rpc.server.netty.work.size: " + workerCount);
    EventLoopGroup bossGroup = null;
    EventLoopGroup workerGroup = null;
    if (useEpoll) {
      bossGroup = new EpollEventLoopGroup(3);
      workerGroup = new EpollEventLoopGroup(workerCount);
    } else {
      bossGroup = new NioEventLoopGroup(3);
      workerGroup = new NioEventLoopGroup(workerCount);
    }
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup);
    if (useEpoll) {
      bootstrap.channel(EpollServerSocketChannel.class);
    } else {
      bootstrap.channel(NioServerSocketChannel.class);
    }
    //bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
    bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.childOption(ChannelOption.SO_LINGER, 0);
    //bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    // bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
    // bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
    bootstrap.childHandler(new Initializer());

    serverChannel = bootstrap.bind(this.bindAddress).sync().channel();
    LOG.info("Netty4RpcServer bind to: " + serverChannel.localAddress());
    allChannels.add(serverChannel);

    this.scheduler.init(new RpcSchedulerContext(this));
  }

  private boolean useEpoll(Configuration conf) {
    // Config to enable native transport. Does not seem to be stable at time of implementation
    // although it is not extensively tested.
    boolean epollEnabled = conf.getBoolean("hbase.rpc.server.nativetransport", true);
    // Use the faster native epoll transport mechanism on linux if enabled
    if (epollEnabled && JVM.isLinux() && JVM.isAmd64()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void start() {
    if (started) {
      return;
    }
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
    allChannels.close().awaitUninterruptibly();
    serverChannel.close();
    scheduler.stop();
    closed.countDown();
  }

  @Override
  public void join() throws InterruptedException {
    closed.await();
  }

  @Override
  public InetSocketAddress getListenerAddress() {
    return ((InetSocketAddress) serverChannel.localAddress());
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
      InetSocketAddress inetSocketAddress = ((InetSocketAddress) channel.remoteAddress());
      this.addr = inetSocketAddress.getAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = inetSocketAddress.getAddress().getHostAddress();
      }
      this.remotePort = inetSocketAddress.getPort();
    }

    void readPreamble(ByteBuf buffer) throws IOException {
      byte[] rpcHead =
          { buffer.readByte(), buffer.readByte(), buffer.readByte(), buffer.readByte() };
      if (!Arrays.equals(HConstants.RPC_HEADER, rpcHead)) {
         doBadPreambleHandling("Expected HEADER="
            + Bytes.toStringBinary(HConstants.RPC_HEADER) + " but received HEADER="
            + Bytes.toStringBinary(rpcHead) + " from " + toString());
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

    Object process(ByteBuffer buf) throws IOException, InterruptedException {
      if (skipInitialSaslHandshake) {
        skipInitialSaslHandshake = false;
        return null;
      }

      if (useSasl) {
        return saslReadAndProcess(buf);
      } else {
        return processOneRpc(buf);
      }
    }

    private Object processOneRpc(ByteBuffer buf) throws IOException, InterruptedException {
      if (connectionHeaderRead) {
        return processRequest(buf);
      } else {
        processConnectionHeader(buf);
        this.connectionHeaderRead = true;
        if (!authorizeConnection()) {
          // Throw FatalConnectionException wrapping ACE so client does right thing and closes
          // down the connection instead of trying to read non-existent retun.
          throw new AccessDeniedException("Connection from " + this + " for service "
              + connectionHeader.getServiceName() + " is unauthorized for user: " + user);
        }
        return null;
      }
    }

    Object processRequest(ByteBuffer buf) throws IOException, InterruptedException {
      long totalRequestSize = buf.limit();
      int offset = 0;
      // Here we read in the header. We avoid having pb
      // do its default 4k allocation for CodedInputStream. We force it to
      // use backing array.
      CodedInputStream cis = CodedInputStream.newInstance(buf.array(), offset, buf.limit());
      int headerSize = cis.readRawVarint32();
      offset = cis.getTotalBytesRead();
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
            new Call(id, this.service, null, null, null, null, this, totalRequestSize, null,
                null);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, callTooBig, CALL_QUEUE_TOO_BIG_EXCEPTION,
          "Call queue is full on " + getListenerAddress()
              + ", is hbase.ipc.server.max.callqueue.size too small?");
        callTooBig.sendResponseIfReady();
        return null;
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
            new Call(id, this.service, null, null, null, null, this, totalRequestSize, null,
                null);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        setupResponse(responseBuffer, readParamsFailedCall, t, msg + "; " + t.getMessage());
        readParamsFailedCall.sendResponseIfReady();
        return null;
      }

      TraceInfo traceInfo =
          header.hasTraceInfo() ? new TraceInfo(header.getTraceInfo().getTraceId(), header
              .getTraceInfo().getParentId()) : null;
      Call call =
          new Call(id, this.service, md, header, param, cellScanner, this, totalRequestSize,
              traceInfo, RpcServer.getRemoteIp());
//      if (!scheduler.dispatch(new CallRunner(Netty4RpcServer.this, call))) {
//        callQueueSize.add(-1 * call.getSize());
//
//        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
//        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
//        InetSocketAddress address = getListenerAddress();
//        setupResponse(responseBuffer, call, CALL_QUEUE_TOO_BIG_EXCEPTION, "Call queue is full on "
//            + (address != null ? address : "(channel closed)") + ", too many items queued ?");
//        call.sendResponseIfReady();
//      }
      return new CallRunner(Netty4RpcServer.this, call);
    }

    private Object saslReadAndProcess(ByteBuffer saslToken) throws IOException, InterruptedException {
      if (saslContextEstablished) {
        if (LOG.isTraceEnabled())
          LOG.trace("Have read input token of size " + saslToken.limit()
              + " for processing by saslServer.unwrap()");

        if (!useWrap) {
          return processOneRpc(saslToken);
        } else {
          byte[] b = saslToken.array();
          byte [] plaintextData = saslServer.unwrap(b, saslToken.position(), saslToken.limit());
          return processUnwrappedData(plaintextData);
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
        return null;
      }
    }

    private Object processUnwrappedData(byte[] inBuf) throws IOException,
    InterruptedException {
      ReadableByteChannel ch = java.nio.channels.Channels.newChannel(new ByteArrayInputStream(inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        int count;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return null;
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
          return null;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          Object result = processOneRpc(unwrappedData);
          unwrappedData = null;
          return result;
        }
      }
    }

    protected int channelRead(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
      int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
          channel.read(buffer) : channelIO(channel, null, buffer);
      return count;
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
        //responder.doRespond(authFailedCall);
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

    ByteBuf responseBB = null;

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
      getConnection().channel.writeAndFlush(this);
    }

    public synchronized void sendResponseIfReady(ChannelFutureListener listener) throws IOException {
      getConnection().channel.writeAndFlush(this).addListener(listener);
    }

  }
  
  private class Initializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      pipeline.addLast("header", new ConnectionHeaderHandler());
      // pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4,
      // 0, 4));
      // pipeline.addLast("decoder", new MessageDecoder());
      pipeline.addLast("decoder", new NettyProtocolDecoder());
      pipeline.addLast("encoder", new MessageEncoder());
      pipeline.addLast("schedulerHandler", new SchedulerHandler());
    }

  }
  
  public class ConnectionHeaderHandler extends ReplayingDecoder<State> {
    // If initial preamble with version and magic has been read or not.
    private boolean connectionPreambleRead = false;
    private Connection connection;

    public ConnectionHeaderHandler() {
      super(State.CHECK_PROTOCOL_VERSION);
    }

    private void readPreamble(ChannelHandlerContext ctx, ByteBuf input) throws IOException {
      if (input.readableBytes() < 6) {
        return;
      }
      connection = new Connection(ctx.channel());
      connection.readPreamble(input);
      ((NettyProtocolDecoder) ctx.pipeline().get("decoder")).setConnection(connection);
      connectionPreambleRead = true;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out)
        throws Exception {
      switch (state()) {
        case CHECK_PROTOCOL_VERSION: {
          readPreamble(ctx, byteBuf);
          if (connectionPreambleRead) {
            break;
          }
          checkpoint(State.READ_AUTH_SCHEMES);
        }
      }
      ctx.pipeline().remove(this);
    }

  }

  enum State {
    CHECK_PROTOCOL_VERSION, READ_AUTH_SCHEMES
  }

  class NettyProtocolDecoder extends ChannelInboundHandlerAdapter {

    private Connection connection;
    ByteBuf cumulation;

    void setConnection(Connection connection) {
      this.connection = connection;
    }

    /**
     * Returns the actual number of readable bytes in the internal cumulative
     * buffer of this decoder. You usually do not need to rely on this value
     * to write a decoder. Use it only when you must use it at your own risk.
     * This method is a shortcut to {@link #internalBuffer() internalBuffer().readableBytes()}.
     */
    protected int actualReadableBytes() {
      return internalBuffer().readableBytes();
    }

    /**
     * Returns the internal cumulative buffer of this decoder. You usually
     * do not need to access the internal buffer directly to write a decoder.
     * Use it only when you must use it at your own risk.
     */
    protected ByteBuf internalBuffer() {
      if (cumulation != null) {
        return cumulation;
      } else {
        return Unpooled.EMPTY_BUFFER;
      }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      allChannels.add(ctx.channel());
      super.channelActive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
      LOG.warn("Unexpected exception from downstream.", e);
      allChannels.remove(ctx.channel());
      ctx.channel().close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      RecyclableArrayList out = RecyclableArrayList.newInstance();
      try {
        if (msg instanceof ByteBuf) {
          ByteBuf data = (ByteBuf) msg;
          if (cumulation == null) {
            cumulation = data;
            try {
              callDecode(ctx, cumulation, out);
            } finally {
              if (cumulation != null && !cumulation.isReadable()) {
                cumulation.release();
                cumulation = null;
              }
            }
          } else {
            try {
              if (cumulation.writerIndex() > cumulation.maxCapacity() - data.readableBytes()) {
                ByteBuf oldCumulation = cumulation;
                cumulation = ctx.alloc().buffer(oldCumulation.readableBytes() + data.readableBytes());
                cumulation.writeBytes(oldCumulation);
                oldCumulation.release();
              }
              cumulation.writeBytes(data);
              callDecode(ctx, cumulation, out);
            } finally {
              if (cumulation != null) {
                if (!cumulation.isReadable()) {
                  cumulation.release();
                  cumulation = null;
                } else {
                  cumulation.discardSomeReadBytes();
                }
              }
              data.release();
            }
          }
        } else {
          out.add(msg);
        }
      } catch (DecoderException e) {
        throw e;
      } catch (Throwable t) {
        throw new DecoderException(t);
      } finally {
        if (!out.isEmpty()) {
          List<Object> results = new ArrayList<Object>();
          for (Object result : out) {
            results.add(result);
          }
          ctx.fireChannelRead(results);
        }
        out.recycle();
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      allChannels.remove(ctx.channel());
      RecyclableArrayList out = RecyclableArrayList.newInstance();
      try {
        if (cumulation != null) {
          callDecode(ctx, cumulation, out);
          decodeLast(ctx, cumulation, out);
        } else {
          decodeLast(ctx, Unpooled.EMPTY_BUFFER, out);
        }
      } catch (DecoderException e) {
        throw e;
      } catch (Exception e) {
        throw new DecoderException(e);
      } finally {
        if (cumulation != null) {
          cumulation.release();
          cumulation = null;
        }

        for (int i = 0; i < out.size(); i++) {
          ctx.fireChannelRead(out.get(i));
        }
        ctx.fireChannelInactive();
      }
    }

    @Override
    public final void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      ByteBuf buf = internalBuffer();
      int readable = buf.readableBytes();
      if (buf.isReadable()) {
        ByteBuf bytes = buf.readBytes(readable);
        buf.release();
        ctx.fireChannelRead(bytes);
      }
      cumulation = null;
      ctx.fireChannelReadComplete();
    }

    /**
     * Called once data should be decoded from the given {@link ByteBuf}. This method will call
     * {@link #decode(ChannelHandlerContext, ByteBuf, List)} as long as decoding should take place.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in  the {@link ByteBuf} from which to read data
     * @param out the {@link List} to which decoded messages should be added
     */
    protected void callDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      try {
        while (in.isReadable()) {
          int outSize = out.size();
          int oldInputLength = in.readableBytes();
          decode(ctx, in, out);

          // Check if this handler was removed before try to continue the loop.
          // If it was removed it is not safe to continue to operate on the buffer
          //
          // See https://github.com/netty/netty/issues/1664
          if (ctx.isRemoved()) {
            break;
          }

          if (outSize == out.size()) {
            if (oldInputLength == in.readableBytes()) {
              break;
            } else {
              continue;
            }
          }

          if (oldInputLength == in.readableBytes()) {
            throw new DecoderException(
                StringUtil.simpleClassName(getClass()) +
                    ".decode() did not read anything but decoded a message.");
          }
        }
      } catch (DecoderException e) {
        throw e;
      } catch (Throwable cause) {
        throw new DecoderException(cause);
      }
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out)
        throws Exception {
      ByteBuffer data = getData(buf);
      if (data != null) {
        Object result = connection.process(data);
        if (result != null) {
          out.add(result);
        }
      }
    }

    private ByteBuffer getData(ByteBuf buf) throws Exception {
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
      // ByteBuffer data = buf.toByteBuffer(buf.readerIndex(), length);
      ByteBuffer data = ByteBuffer.allocate(length);
      buf.readBytes(data);
      data.flip();
      // buf.skipBytes(length);
      return data;
    }

    /**
     * Is called one last time when the {@link ChannelHandlerContext} goes in-active. Which means the
     * {@link #channelInactive(ChannelHandlerContext)} was triggered.
     * By default this will just call {@link #decode(ChannelHandlerContext, ByteBuf, List)} but sub-classes may
     * override this for some special cleanup operation.
     */
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      decode(ctx, in, out);
    }
  }

  class MessageDecoder extends ChannelInboundHandlerAdapter {
    // If the connection header has been read or not.
    private boolean connectionHeaderRead = false;

    private Connection connection;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      allChannels.add(ctx.channel());
      super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      ByteBuf input = (ByteBuf) msg;
      try {
        byte[] data = new byte[input.readableBytes()];
        input.readBytes(data, 0, data.length);
        ByteBuffer buf = ByteBuffer.wrap(data);
        if (!connectionHeaderRead) {
          connection.processConnectionHeader(buf);
          connectionHeaderRead = true;
        } else {
          connection.processRequest(buf);
        }
      } finally {
        input.release();
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      allChannels.remove(ctx.channel());
      super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
      LOG.warn("Unexpected exception from downstream.", e);
      allChannels.remove(ctx.channel());
      ctx.channel().close();
    }

  }
  
  class SchedulerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (!(msg instanceof CallRunner) && !(msg instanceof List)) {
        LOG.error("receive message error,only support RequestWrapper || List");
        throw new Exception("receive message error,only support RequestWrapper || List");
      }
      if (msg instanceof List) {
        List messages = (List) msg;
        for (Object messageObject : messages) {
          handleSingleRequest(ctx, messageObject);
        }
      } else {
        handleSingleRequest(ctx, msg);
      }
    }

    private void handleSingleRequest(final ChannelHandlerContext ctx, final Object message)
        throws IOException, InterruptedException {
      CallRunner task = (CallRunner) message;
      if (!scheduler.dispatch(task)) {
        callQueueSize.add(-1 * task.getRpcCall().getSize());
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        InetSocketAddress address = getListenerAddress();
        task.getRpcCall().setResponse(
          null,
          null,
          CALL_QUEUE_TOO_BIG_EXCEPTION,
          "Call queue is full on " + (address != null ? address : "(channel closed)")
              + ", too many items queued ?");
        task.getRpcCall().sendResponseIfReady();
      }
    }

  }

  class MessageEncoder extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
      final Call call = (Call) msg;
//      BufferChain buf = call.response;
//      ByteBuffer[] buffers = buf.getBuffers();
//      ByteBuf encoded = ctx.alloc().buffer(buf.size());
//      for (ByteBuffer bb : buffers) {
//        encoded.writeBytes(bb);
//      }
//      ctx.write(encoded, promise).addListener(new CallWriteListener(call));
      ByteBuf response = Unpooled.wrappedBuffer(call.response.getBuffers());
      ctx.write(response, promise).addListener(new CallWriteListener(call));
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
      if (future.isSuccess()) {
        metrics.sentBytes(call.response.size());
      }
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
