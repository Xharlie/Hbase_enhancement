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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.io.BoundedByteBufferPool;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.VersionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.TraceInfo;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public abstract class RpcServer implements RpcServerInterface, ConfigurationObserver {
  public static final Log LOG = LogFactory.getLog(RpcServer.class);
  protected static final CallQueueTooBigException CALL_QUEUE_TOO_BIG_EXCEPTION
      = new CallQueueTooBigException();

  private final boolean authorize;
  protected boolean isSecurityEnabled;

  public static final byte CURRENT_VERSION = 0;

  /**
   * How many calls/handler are allowed in the queue.
   */
  protected static final int DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER = 10;

  /**
   * The maximum size that we can hold in the RPC queue
   */
  protected static final int DEFAULT_MAX_CALLQUEUE_SIZE = 1024 * 1024 * 1024;

  protected static long maxRpcSize;
  protected static final long DEFAULT_MAX_RPC_SIZE = 100 * 1024 * 1024L;
  protected final String CONF_MAX_RPC_SIZE = "hbase.ipc.max.rpc.size";

  protected int slowCallLimit;
  protected long incrementPeriod;
  protected AtomicLong totalSlowCalls;
  protected AtomicLong totalCalls;

  protected final IPCUtil ipcUtil;

  protected static final String AUTH_FAILED_FOR = "Auth failed for ";
  protected static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  protected static final Log AUDITLOG = LogFactory.getLog("SecurityLogger." +
    Server.class.getName());
  protected SecretManager<TokenIdentifier> secretManager;
  protected ServiceAuthorizationManager authManager;

  /** This is set to Call object before Handler invokes an RPC and ybdie
   * after the call returns.
   */
  protected static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

  /** Keeps MonitoredRPCHandler per handler thread. */
  protected static final ThreadLocal<MonitoredRPCHandler> MONITORED_RPC
      = new ThreadLocal<MonitoredRPCHandler>();

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

  protected MetricsHBaseServer metrics;

  protected final Configuration conf;

  protected int maxQueueSize;

  /**
   * This flag is used to indicate to sub threads when they should go down.  When we call
   * {@link #start()}, all threads started will consult this flag on whether they should
   * keep going.  It is set to false when {@link #stop()} is called.
   */
  volatile boolean running = true;

  /**
   * This flag is set to true after all threads are up and 'running' and the server is then opened
   * for business by the call to {@link #start()}.
   */
  volatile boolean started = false;

  /**
   * This is a running count of the size of all outstanding calls by size.
   */
  protected final Counter callQueueSize = new Counter();

  protected AuthenticationTokenSecretManager authTokenSecretMgr = null;
  protected int numConnections = 0;

  protected HBaseRPCErrorHandler errorHandler = null;

  private static final String WARN_RESPONSE_TIME = "hbase.ipc.warn.response.time";
  private static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";
  // log long queue time no quicker than this interval, in milliseconds
  private static final String WARN_LONG_QUEUE_TIME_MIN_INTERVAL =
      "hbase.ipc.warn.longqueuetime.interval.min";

  /** Default value for above params */
  private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;
  private static final long DEFAULT_WARN_LONG_QUEUE_TIME_MIN_INTERVAL = 1000L;

  private static final String WARN_LONG_QUEUE_TIME_TAG = "TooSlow-LongQueueTime";
  private static final String WARN_LONG_PROCESS_TIME_TAG = "TooSlow-LongProcessTime";
  private static final String WARN_LARGE_RESPONSE_SIZE_TAG = "TooLarge";

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  protected final int warnResponseTime;
  protected final int warnResponseSize;
  protected final long warnLongQueueTimeInterval;
  private volatile long timeOfLastLongQueueTime = -1L;
  protected final Server server;
  protected final List<BlockingServiceAndInterface> services;

  protected final RpcScheduler scheduler;

  protected UserProvider userProvider;

  protected final BoundedByteBufferPool reservoir;

  public RpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress bindAddress, Configuration conf,
      RpcScheduler scheduler) {
    this.reservoir = new BoundedByteBufferPool(
      conf.getInt("hbase.ipc.server.reservoir.max.buffer.size",  1024 * 1024),
      conf.getInt("hbase.ipc.server.reservoir.initial.buffer.size", 16 * 1024),
      // Make the max twice the number of handlers to be safe.
      conf.getInt("hbase.ipc.server.reservoir.initial.max",
        conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
          HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT) * 2));
    this.server = server;
    this.services = services;
    this.conf = conf;
    this.maxQueueSize =
        this.conf.getInt("hbase.ipc.server.max.callqueue.size", DEFAULT_MAX_CALLQUEUE_SIZE);

    this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME, DEFAULT_WARN_RESPONSE_TIME);
    this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE, DEFAULT_WARN_RESPONSE_SIZE);
    this.warnLongQueueTimeInterval =
        conf.getLong(WARN_LONG_QUEUE_TIME_MIN_INTERVAL, DEFAULT_WARN_LONG_QUEUE_TIME_MIN_INTERVAL);
    this.metrics = new MetricsHBaseServer(name, new MetricsHBaseServerWrapperImpl(this));

    // mark a call slow when its total time is longer than 10 secs
    this.slowCallLimit = this.conf.getInt("hbase.ipc.server.slow.call.limit", 10 * 1000);
    this.incrementPeriod = this.conf.getLong("hbase.ipc.server.metrics.inc.period", 60 * 60 * 1000L);
    this.totalSlowCalls = new AtomicLong(0);
    this.totalCalls = new AtomicLong(0);

    // set max rpc size
    maxRpcSize = this.conf.getLong(CONF_MAX_RPC_SIZE, DEFAULT_MAX_RPC_SIZE);

    this.ipcUtil = new IPCUtil(conf);

    this.authorize = conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
    this.userProvider = UserProvider.instantiate(conf);
    this.isSecurityEnabled = userProvider.isHBaseSecurityEnabled();
    if (isSecurityEnabled) {
      HBaseSaslRpcServer.init(conf);
    }
    this.scheduler = scheduler;
  }

  /**
   * Datastructure that holds all necessary to a method invocation and then afterward, carries
   * the result.
   */
  public abstract class Call implements RpcCallContext {
    protected int id; // the client's call id
    protected BlockingService service;
    protected MethodDescriptor md;
    protected RequestHeader header;
    protected Message param; // the parameter passed
    // Optional cell data passed outside of protobufs.
    protected CellScanner cellScanner;
    protected Connection connection; // connection to client
    protected long timestamp; // the time received when response is null
    // the time served when response is not null
    /**
     * Chain of buffers to send as response.
     */
    protected BufferChain response;
    protected long size; // size of current call
    protected boolean isError;
    protected TraceInfo tinfo;
    protected ByteBuffer cellBlock = null;

    protected User user;
    protected InetAddress remoteAddress;

    public Call(int id, final BlockingService service, final MethodDescriptor md,
        RequestHeader header, Message param, CellScanner cellScanner, Connection connection,
        long size, TraceInfo tinfo, final InetAddress remoteAddress) {
      this.id = id;
      this.service = service;
      this.md = md;
      this.header = header;
      this.param = param;
      this.cellScanner = cellScanner;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
      this.isError = false;
      this.size = size;
      this.tinfo = tinfo;
      this.user = connection.user == null ? null : userProvider.create(connection.user);
      this.remoteAddress = remoteAddress;
    }

    public long getSize() {
      return this.size;
    }

    public RequestHeader getHeader() {
      return this.header;
    }

    public UserGroupInformation getRemoteUser() {
      return connection.user;
    }

    @Override
    public User getRequestUser() {
      return user;
    }

    @Override
    public String getRequestUserName() {
      User user = getRequestUser();
      return user == null ? null : user.getShortName();
    }

    @Override
    public InetAddress getRemoteAddress() {
      return remoteAddress;
    }

    @Override
    public VersionInfo getClientVersionInfo() {
      return connection.getVersionInfo();
    }

    public TraceInfo getTinfo() {
      return tinfo;
    }

    public BlockingService getService() {
      return service;
    }

    public MethodDescriptor getMethodDescriptor() {
      return md;
    }

    public Message getParam() {
      return param;
    }

    public CellScanner getCellScanner() {
      return cellScanner;
    }

    public long getTimestamp() {
      return timestamp;
    }

    protected synchronized void setSaslTokenResponse(ByteBuffer response) {
      this.response = new BufferChain(response);
    }

    @Override
    public boolean isClientCellBlockSupport() {
      return this.connection != null && this.connection.codec != null;
    }

    @Override
    public String toString() {
      return toShortString() + " param: "
          + (this.param != null ? ProtobufUtil.getShortTextFormat(this.param) : "")
          + " connection: " + connection.toString();
    }

    /*
     * Short string representation without param info because param itself could be huge depends on
     * the payload of a command
     */
    public String toShortString() {
      String serviceName =
          this.connection.service != null ? this.connection.service.getDescriptorForType()
              .getName() : "null";
      return "callId: " + this.id + " service: " + serviceName + " methodName: "
          + ((this.md != null) ? this.md.getName() : "n/a") + " size: "
          + StringUtils.TraditionalBinaryPrefix.long2String(this.size, "", 1) + " connection: "
          + connection.toString();
    }

    public String toTraceString() {
      String serviceName =
          this.connection.service != null ? this.connection.service.getDescriptorForType()
              .getName() : "";
      String methodName = (this.md != null) ? this.md.getName() : "";
      return serviceName + "." + methodName;
    }

    public synchronized void setResponse(Object m, final CellScanner cells,
        Throwable t, String errorMsg) {
      if (this.isError) return;
      if (t != null) this.isError = true;
      BufferChain bc = null;
      try {
        ResponseHeader.Builder headerBuilder = ResponseHeader.newBuilder();
        // Presume it a pb Message.  Could be null.
        Message result = (Message)m;
        // Call id.
        headerBuilder.setCallId(this.id);
        if (t != null) {
          ExceptionResponse.Builder exceptionBuilder = ExceptionResponse.newBuilder();
          exceptionBuilder.setExceptionClassName(t.getClass().getName());
          exceptionBuilder.setStackTrace(errorMsg);
          exceptionBuilder.setDoNotRetry(t instanceof DoNotRetryIOException);
          if (t instanceof RegionMovedException) {
            // Special casing for this exception.  This is only one carrying a payload.
            // Do this instead of build a generic system for allowing exceptions carry
            // any kind of payload.
            RegionMovedException rme = (RegionMovedException)t;
            exceptionBuilder.setHostname(rme.getHostname());
            exceptionBuilder.setPort(rme.getPort());
          }
          // Set the exception as the result of the method invocation.
          headerBuilder.setException(exceptionBuilder.build());
        }
        // Pass reservoir to buildCellBlock. Keep reference to returne so can add it back to the
        // reservoir when finished. This is hacky and the hack is not contained but benefits are
        // high when we can avoid a big buffer allocation on each rpc.
        this.cellBlock = ipcUtil.buildCellBlock(this.connection.codec,
          this.connection.compressionCodec, cells, reservoir);
        if (this.cellBlock != null) {
          CellBlockMeta.Builder cellBlockBuilder = CellBlockMeta.newBuilder();
          // Presumes the cellBlock bytebuffer has been flipped so limit has total size in it.
          cellBlockBuilder.setLength(this.cellBlock.limit());
          headerBuilder.setCellBlockMeta(cellBlockBuilder.build());
        }
        Message header = headerBuilder.build();

        // Organize the response as a set of bytebuffers rather than collect it all together inside
        // one big byte array; save on allocations.
        ByteBuffer bbHeader = IPCUtil.getDelimitedMessageAsByteBuffer(header);
        ByteBuffer bbResult = IPCUtil.getDelimitedMessageAsByteBuffer(result);
        int totalSize = bbHeader.capacity() + (bbResult == null? 0: bbResult.limit()) +
          (this.cellBlock == null? 0: this.cellBlock.limit());
        ByteBuffer bbTotalSize = ByteBuffer.wrap(Bytes.toBytes(totalSize));
        bc = new BufferChain(bbTotalSize, bbHeader, bbResult, this.cellBlock);
        if (connection.useWrap) {
          bc = wrapWithSasl(bc);
        }
      } catch (IOException e) {
        LOG.warn("Exception while creating response " + e);
      }
      this.response = bc;
    }

    BufferChain wrapWithSasl(BufferChain bc)
        throws IOException {
      if (!this.connection.useSasl) return bc;
      // Looks like no way around this; saslserver wants a byte array.  I have to make it one.
      // THIS IS A BIG UGLY COPY.
      byte [] responseBytes = bc.getBytes();
      byte [] token;
      // synchronization may be needed since there can be multiple Handler
      // threads using saslServer to wrap responses.
      synchronized (connection.saslServer) {
        token = connection.saslServer.wrap(responseBytes, 0, responseBytes.length);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      }

      ByteBuffer bbTokenLength = ByteBuffer.wrap(Bytes.toBytes(token.length));
      ByteBuffer bbTokenBytes = ByteBuffer.wrap(token);
      return new BufferChain(bbTokenLength, bbTokenBytes);
    }

    public Connection getConnect() {
      return connection;
    }

    public abstract InetAddress getInetAddress();

    public abstract void sendResponseIfReady() throws IOException;

  }

  /** Reads calls from a connection and queues them for handling. */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="VO_VOLATILE_INCREMENT",
      justification="False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
  public abstract class Connection {
    // If initial preamble with version and magic has been read or not.
    boolean connectionPreambleRead = false;
    // If the connection header has been read or not.
    boolean connectionHeaderRead = false;

    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    String hostAddress;
    int remotePort;
    InetAddress addr;

    ConnectionHeader connectionHeader;
    /**
     * Codec the client asked use.
     */
    Codec codec;
    /**
     * Compression codec the client asked us use.
     */
    CompressionCodec compressionCodec;
    BlockingService service;
    UserGroupInformation user = null;
    AuthMethod authMethod;
    boolean saslContextEstablished;
    boolean skipInitialSaslHandshake;
    boolean useSasl;

    // Fake 'call' for failed authorization response
    static final int AUTHORIZATION_FAILED_CALLID = -1;
    // Fake 'call' for SASL context setup
    static final int SASL_CALLID = -33;

    SaslServer saslServer;
    boolean useWrap = false;
    ByteArrayOutputStream authFailedResponse = new ByteArrayOutputStream();
    ByteBuffer unwrappedData;
    // When is this set?  FindBugs wants to know!  Says NP
    ByteBuffer unwrappedDataLengthBuffer = ByteBuffer.allocate(4);

    public UserGroupInformation attemptingUser = null; // user name before auth

    public Connection() {
    }

    public InetAddress getHostInetAddress() {
      return addr;
    }

    public void setAttemptingUser(UserGroupInformation attemptingUser) {
      this.attemptingUser = attemptingUser;
    }

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort;
    }

    public UserGroupInformation getUser() {
      return user;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public int getRemotePort() {
      return remotePort;
    }

    public VersionInfo getVersionInfo() {
      if (connectionHeader.hasVersionInfo()) {
        return connectionHeader.getVersionInfo();
      }
      return null;
    }

    // Reads the connection header following version
    void processConnectionHeader(ByteBuffer buf) throws IOException {
      this.connectionHeader = ConnectionHeader.parseFrom(
        new ByteBufferInputStream(buf));
      String serviceName = connectionHeader.getServiceName();
      if (serviceName == null) throw new EmptyServiceNameException();
      this.service = getService(services, serviceName);
      if (this.service == null) throw new UnknownServiceException(serviceName);
      setupCellBlockCodecs(this.connectionHeader);
      UserGroupInformation protocolUser = createUser(connectionHeader);
      if (!useSasl) {
        user = protocolUser;
        if (user != null) {
          user.setAuthenticationMethod(AuthMethod.SIMPLE.authenticationMethod);
        }
      } else {
        // user is authenticated
        user.setAuthenticationMethod(authMethod.authenticationMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However,
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(user.getUserName()))) {
          if (authMethod == AuthMethod.DIGEST) {
            // Not allowed to doAs if token authentication is used
            throw new AccessDeniedException("Authenticated user (" + user
                + ") doesn't match what the client claims to be ("
                + protocolUser + ")");
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = user;
            user = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
            // Now the user is a proxy user, set Authentication method Proxy.
            user.setAuthenticationMethod(AuthenticationMethod.PROXY);
          }
        }
      }
      if (connectionHeader.hasVersionInfo()) {
        AUDITLOG.info("Connection from " + this.hostAddress + " port: " + this.remotePort
            + " with version info: "
            + TextFormat.shortDebugString(connectionHeader.getVersionInfo()));
      } else {
        AUDITLOG.info("Connection from " + this.hostAddress + " port: " + this.remotePort
            + " with unknown version info");
      }
    }

    protected String getFatalConnectionString(final int version, final byte authByte) {
      return "serverVersion=" + CURRENT_VERSION + ", clientVersion=" + version + ", authMethod="
          + authByte + ", authSupported=" + (authMethod != null) + " from " + toString();
    }

    protected UserGroupInformation getAuthorizedUgi(String authorizedId)
        throws IOException {
      if (authMethod == AuthMethod.DIGEST) {
        TokenIdentifier tokenId = HBaseSaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        UserGroupInformation ugi = tokenId.getUser();
        if (ugi == null) {
          throw new AccessDeniedException(
              "Can't retrieve username from tokenIdentifier.");
        }
        ugi.addTokenIdentifier(tokenId);
        return ugi;
      } else {
        return UserGroupInformation.createRemoteUser(authorizedId);
      }
    }

    /**
     * Set up cell block codecs
     * @throws FatalConnectionException
     */
    void setupCellBlockCodecs(final ConnectionHeader header) throws FatalConnectionException {
      // TODO: Plug in other supported decoders.
      if (!header.hasCellBlockCodecClass()) return;
      String className = header.getCellBlockCodecClass();
      if (className == null || className.length() == 0) return;
      try {
        this.codec = (Codec) Class.forName(className).newInstance();
      } catch (Exception e) {
        throw new UnsupportedCellCodecException(className, e);
      }
      if (!header.hasCellBlockCompressorClass()) return;
      className = header.getCellBlockCompressorClass();
      try {
        this.compressionCodec = (CompressionCodec) Class.forName(className).newInstance();
      } catch (Exception e) {
        throw new UnsupportedCompressionCodecException(className, e);
      }
    }

    UserGroupInformation createUser(ConnectionHeader head) {
      UserGroupInformation ugi = null;

      if (!head.hasUserInfo()) {
        return null;
      }
      UserInformation userInfoProto = head.getUserInfo();
      String effectiveUser = null;
      if (userInfoProto.hasEffectiveUser()) {
        effectiveUser = userInfoProto.getEffectiveUser();
      }
      String realUser = null;
      if (userInfoProto.hasRealUser()) {
        realUser = userInfoProto.getRealUser();
      }
      if (effectiveUser != null) {
        if (realUser != null) {
          UserGroupInformation realUserUgi =
              UserGroupInformation.createRemoteUser(realUser);
          ugi = UserGroupInformation.createProxyUser(effectiveUser, realUserUgi);
        } else {
          ugi = UserGroupInformation.createRemoteUser(effectiveUser);
        }
      }
      return ugi;
    }

    void disposeSasl() {
      if (saslServer != null) {
        try {
          saslServer.dispose();
          saslServer = null;
        } catch (SaslException ignored) {
          // Ignored. This is being disposed of anyway.
        }
      }
    }

    public abstract boolean isConnectionOpen();

  }

  /**
   * Datastructure for passing a {@link BlockingService} and its associated class of
   * protobuf service interface.  For example, a server that fielded what is defined
   * in the client protobuf service would pass in an implementation of the client blocking service
   * and then its ClientService.BlockingInterface.class.  Used checking connection setup.
   */
  public static class BlockingServiceAndInterface {
    private final BlockingService service;
    private final Class<?> serviceInterface;
    public BlockingServiceAndInterface(final BlockingService service,
        final Class<?> serviceInterface) {
      this.service = service;
      this.serviceInterface = serviceInterface;
    }
    public Class<?> getServiceInterface() {
      return this.serviceInterface;
    }
    public BlockingService getBlockingService() {
      return this.service;
    }
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public boolean isStarted() {
    return this.started;
  }

  @Override
  public void refreshAuthManager(PolicyProvider pp) {
    // Ignore warnings that this should be accessed in a static way instead of via an instance;
    // it'll break if you go via static route.
    this.authManager.refresh(this.conf, pp);
  }

  protected AuthenticationTokenSecretManager createSecretManager() {
    if (!isSecurityEnabled) return null;
    if (server == null) return null;
    Configuration conf = server.getConfiguration();
    long keyUpdateInterval =
        conf.getLong("hbase.auth.key.update.interval", 24*60*60*1000);
    long maxAge =
        conf.getLong("hbase.auth.token.max.lifetime", 7*24*60*60*1000);
    return new AuthenticationTokenSecretManager(conf, server.getZooKeeper(),
        server.getServerName().toString(), keyUpdateInterval, maxAge);
  }

  public SecretManager<? extends TokenIdentifier> getSecretManager() {
    return this.secretManager;
  }

  @SuppressWarnings("unchecked")
  public void setSecretManager(SecretManager<? extends TokenIdentifier> secretManager) {
    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;
  }

  /**
   * This is a server side method, which is invoked over RPC. On success
   * the return response has protobuf response payload. On failure, the
   * exception name and the stack trace are returned in the protobuf response.
   */
  @Override
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
      Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
  throws IOException {
    try {
      status.setRPC(md.getName(), new Object[]{param}, receiveTime);
      // TODO: Review after we add in encoded data blocks.
      status.setRPCPacket(param);
      status.resume("Servicing call");
      //get an instance of the method arg type
      long startTime = System.currentTimeMillis();
      PayloadCarryingRpcController controller = new PayloadCarryingRpcController(cellScanner);
      Message result = service.callBlockingMethod(md, controller, param);
      long endTime = System.currentTimeMillis();
      int processingTime = (int) (endTime - startTime);
      int qTime = (int) (startTime - receiveTime);
      int totalTime = (int) (endTime - receiveTime);
      if (LOG.isTraceEnabled()) {
        LOG.trace(CurCall.get().toString() +
            ", response " + TextFormat.shortDebugString(result) +
            " queueTime: " + qTime +
            " processingTime: " + processingTime +
            " totalTime: " + totalTime);
      }
      metrics.dequeuedCall(qTime);
      metrics.processedCall(processingTime);
      metrics.totalCall(totalTime);
      collectTotalCalls(totalTime);
      long responseSize = result.getSerializedSize();
      // log any RPC responses that are slower than the configured warn
      // response time or larger than configured warning size
      boolean longProcessTime = (processingTime > warnResponseTime && warnResponseTime > -1);
      boolean longQueueTime =
          (!longProcessTime && totalTime > warnResponseTime && warnResponseTime > -1);
      boolean tooLarge = (responseSize > warnResponseSize && warnResponseSize > -1);
      String warnCategory;
      if (longProcessTime) {
        warnCategory = WARN_LONG_PROCESS_TIME_TAG;
      } else if (longQueueTime) {
        warnCategory = WARN_LONG_QUEUE_TIME_TAG;
      } else if (tooLarge) {
        warnCategory = WARN_LARGE_RESPONSE_SIZE_TAG;
      } else {
        warnCategory = null;
      }
      if (warnCategory != null) {
        // when tagging, we let TooLarge trump TooSmall to keep output simple
        // note that large responses will often also be slow.
        logResponse(param,
            md.getName(), md.getName() + "(" + param.getClass().getName() + ")",
            warnCategory,
            status.getClient(), startTime, processingTime, qTime,
            responseSize);
      }
      return new Pair<Message, CellScanner>(result, controller.cellScanner());
    } catch (Throwable e) {
      // The above callBlockingMethod will always return a SE.  Strip the SE wrapper before
      // putting it on the wire.  Its needed to adhere to the pb Service Interface but we don't
      // need to pass it over the wire.
      if (e instanceof ServiceException) e = e.getCause();

      // increment the number of requests that were exceptions.
      metrics.exception(e);

      if (e instanceof LinkageError) throw new DoNotRetryIOException(e);
      if (e instanceof IOException) throw (IOException)e;
      LOG.error("Unexpected throwable object ", e);
      throw new IOException(e.getMessage(), e);
    }
  }

  private void collectTotalCalls(int totalTime) {
    if(totalTime > this.slowCallLimit) {
      this.totalSlowCalls.incrementAndGet();
    }

    this.totalCalls.incrementAndGet();
  }

  public long getIncrementPeriod() {
    return this.incrementPeriod;
  }

  public long getTotalSlowRpcCalls() {
    return this.totalSlowCalls.get();
  }

  public long getTotalRpcCalls() {
    return this.totalCalls.get();
  }

  /**
   * Logs an RPC response to the LOG file, producing valid JSON objects for
   * client Operations.
   * @param param The parameters received in the call.
   * @param methodName The name of the method invoked
   * @param call The string representation of the call
   * @param tag  The tag that will be used to indicate this event in the log.
   * @param clientAddress   The address of the client who made this call.
   * @param startTime       The time that the call was initiated, in ms.
   * @param processingTime  The duration that the call took to run, in ms.
   * @param qTime           The duration that the call spent on the queue
   *                        prior to being initiated, in ms.
   * @param responseSize    The size in bytes of the response buffer.
   */
  void logResponse(Message param, String methodName, String call, String tag,
      String clientAddress, long startTime, int processingTime, int qTime,
      long responseSize)
          throws IOException {
    // base information that is reported regardless of type of call
    Map<String, Object> responseInfo = new HashMap<String, Object>();
    responseInfo.put("starttimems", startTime);
    responseInfo.put("processingtimems", processingTime);
    responseInfo.put("queuetimems", qTime);
    responseInfo.put("responsesize", responseSize);
    responseInfo.put("client", clientAddress);
    responseInfo.put("class", server == null? "": server.getClass().getSimpleName());
    responseInfo.put("method", methodName);
    responseInfo.put("call", call);
    responseInfo.put("param", ProtobufUtil.getShortTextFormat(param));
    if (param instanceof ClientProtos.ScanRequest) {
      ClientProtos.ScanRequest request = ((ClientProtos.ScanRequest) param);
      if (request.hasScannerId()) {
        long scannerId = request.getScannerId();
        String scanDetails = rsRpcServices.getScanDetailsWithId(scannerId);
        if (scanDetails != null) {
          responseInfo.put("scandetails", scanDetails);
        }
      }
    }
    String logMessage = "(response" + tag + "): " + MAPPER.writeValueAsString(responseInfo);
    if (tag.equals(WARN_LONG_QUEUE_TIME_TAG)) {
      // Prevent too much log when something went wrong and caused long queue time for each call
      boolean skipLog = true;
      if (timeOfLastLongQueueTime < 0L
          || (System.currentTimeMillis() - timeOfLastLongQueueTime >= warnLongQueueTimeInterval)) {
        skipLog = false;
      }
      if (skipLog) {
        return;
      }
      LOG.warn(logMessage);
      timeOfLastLongQueueTime = System.currentTimeMillis();
    } else {
      LOG.warn(logMessage);
    }
  }

  /**
   * Set the handler for calling out of RPC for error conditions.
   * @param handler the handler implementation
   */
  @Override
  public void setErrorHandler(HBaseRPCErrorHandler handler) {
    this.errorHandler = handler;
  }

  @Override
  public HBaseRPCErrorHandler getErrorHandler() {
    return this.errorHandler;
  }

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  @Override
  public MetricsHBaseServer getMetrics() {
    return metrics;
  }

  @Override
  public void addCallSize(final long diff) {
    this.callQueueSize.add(diff);
  }

  /**
   * Authorize the incoming client connection.
   *
   * @param user client user
   * @param connection incoming connection
   * @param addr InetAddress of incoming connection
   * @throws org.apache.hadoop.security.authorize.AuthorizationException
   *         when the client isn't authorized to talk the protocol
   */
  public void authorize(UserGroupInformation user, ConnectionHeader connection, InetAddress addr)
  throws AuthorizationException {
    if (authorize) {
      Class<?> c = getServiceInterface(services, connection.getServiceName());
      this.authManager.authorize(user != null ? user : null, c, getConf(), addr);
    }
  }

  /**
   * Needed for features such as delayed calls.  We need to be able to store the current call
   * so that we can complete it later or ask questions of what is supported by the current ongoing
   * call.
   * @return An RpcCallContext backed by the currently ongoing call (gotten from a thread local)
   */
  public static RpcCallContext getCurrentCall() {
    return CurCall.get();
  }

  public static boolean isInRpcCallContext() {
    return CurCall.get() != null;
  }

  /**
   * Returns the user credentials associated with the current RPC request or
   * <code>null</code> if no credentials were provided.
   * @return A User
   */
  public static User getRequestUser() {
    RpcCallContext ctx = getCurrentCall();
    return ctx == null? null: ctx.getRequestUser();
  }

  /**
   * Returns the username for any user associated with the current RPC
   * request or <code>null</code> if no user is set.
   */
  public static String getRequestUserName() {
    User user = getRequestUser();
    return user == null? null: user.getShortName();
  }

  /**
   * @return Address of remote client if a request is ongoing, else null
   */
  public static InetAddress getRemoteAddress() {
    RpcCallContext ctx = getCurrentCall();
    return ctx == null? null: ctx.getRemoteAddress();
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services Available service instances
   * @return Matching BlockingServiceAndInterface pair
   */
  protected static BlockingServiceAndInterface getServiceAndInterface(
      final List<BlockingServiceAndInterface> services, final String serviceName) {
    for (BlockingServiceAndInterface bs : services) {
      if (bs.getBlockingService().getDescriptorForType().getName().equals(serviceName)) {
        return bs;
      }
    }
    return null;
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services Available services and their service interfaces.
   * @return Service interface class for <code>serviceName</code>
   */
  protected static Class<?> getServiceInterface(
      final List<BlockingServiceAndInterface> services,
      final String serviceName) {
    BlockingServiceAndInterface bsasi =
        getServiceAndInterface(services, serviceName);
    return bsasi == null? null: bsasi.getServiceInterface();
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services Available services and their service interfaces.
   * @return BlockingService that goes with the passed <code>serviceName</code>
   */
  protected static BlockingService getService(
      final List<BlockingServiceAndInterface> services,
      final String serviceName) {
    BlockingServiceAndInterface bsasi =
        getServiceAndInterface(services, serviceName);
    return bsasi == null? null: bsasi.getBlockingService();
  }

  static MonitoredRPCHandler getStatus() {
    // It is ugly the way we park status up in RpcServer.  Let it be for now.  TODO.
    MonitoredRPCHandler status = RpcServer.MONITORED_RPC.get();
    if (status != null) {
      return status;
    }
    status = TaskMonitor.get().createRPCStatus(Thread.currentThread().getName());
    status.pause("Waiting for a call");
    RpcServer.MONITORED_RPC.set(status);
    return status;
  }

  /** Returns the remote side ip address when invoked inside an RPC
   *  Returns null incase of an error.
   *  @return InetAddress
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null) {
      return call.getInetAddress();
    }
    return null;
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  static int NIO_BUFFER_LIMIT = 64 * 1024; //should not be more than 64KB.

  /**
   * Helper for {@link #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)}
   * and {@link #channelWrite(GatheringByteChannel, BufferChain)}. Only
   * one of readCh or writeCh should be non-null.
   *
   * @param readCh read channel
   * @param writeCh write channel
   * @param buf buffer to read or write into/out of
   * @return bytes written
   * @throws java.io.IOException e
   * @see #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)
   * @see #channelWrite(GatheringByteChannel, BufferChain)
   */
  static int channelIO(ReadableByteChannel readCh,
                               WritableByteChannel writeCh,
                               ByteBuffer buf) throws IOException {

    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;

    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);

        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);
      }
    }

    int nBytes = initialRemaining - buf.remaining();
    return (nBytes > 0) ? nBytes : ret;
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
  public RpcScheduler getScheduler() {
    return scheduler;
  }

  public long getTotalQueueSize() {
    return callQueueSize.get();
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    int newSlowCallLimit = newConf.getInt("hbase.ipc.server.slow.call.limit", 10 * 1000);
    if(newSlowCallLimit != this.slowCallLimit) {
      LOG.info("Slow call limit changed from " + slowCallLimit
          + " ms to " + newSlowCallLimit + " ms.");
      this.slowCallLimit = newSlowCallLimit;
    } else {
      LOG.info("Slow call limit not changed: " + slowCallLimit + " ms.");
    }

    long newIncrementPeriod = newConf.getLong("hbase.ipc.server.metrics.inc.period", 60 * 60 * 1000L);
    if(newIncrementPeriod != this.incrementPeriod) {
      LOG.info("Increment period for metrics changed from " + incrementPeriod
          + " ms to " + newIncrementPeriod + " ms.");
      this.incrementPeriod = newIncrementPeriod;
    } else {
      LOG.info("Increment period for metrics not changed: " + incrementPeriod + " ms.");
    }

    long newMaxRpcSize = newConf.getLong(CONF_MAX_RPC_SIZE, DEFAULT_MAX_RPC_SIZE);
    if(newMaxRpcSize != maxRpcSize) {
      LOG.info("Max RPC size changed from " + maxRpcSize
          + " bytes to " + newMaxRpcSize + " bytes.");
      maxRpcSize = newMaxRpcSize;
    } else {
      LOG.info("Max RPC size not changed: " + maxRpcSize + " bytes.");
    }
  }

  abstract public int getNumOpenConnections();

  abstract public long getResponseQueueSize();

  abstract public int getResponseQueueLength();

  private RSRpcServices rsRpcServices;

  @Override
  public void setRsRpcServices(RSRpcServices rsRpcServices) {
    this.rsRpcServices = rsRpcServices;
  }

}
