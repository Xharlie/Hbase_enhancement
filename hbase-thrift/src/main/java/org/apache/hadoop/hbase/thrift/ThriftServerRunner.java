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

package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.util.Bytes.getBytes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

import com.etao.hadoop.hbase.queue.client.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.security.SecurityUtil;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.thrift.CallQueue.Call;
import org.apache.hadoop.hbase.thrift.generated.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConnectionCache;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.SaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServlet;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * ThriftServerRunner - this class starts up a Thrift server which implements
 * the Hbase API specified in the Hbase.thrift IDL file.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class ThriftServerRunner implements Runnable {

  private static final Log LOG = LogFactory.getLog(ThriftServerRunner.class);

  static final String SERVER_TYPE_CONF_KEY =
      "hbase.regionserver.thrift.server.type";

  static final String BIND_CONF_KEY = "hbase.regionserver.thrift.ipaddress";
  static final String COMPACT_CONF_KEY = "hbase.regionserver.thrift.compact";
  static final String FRAMED_CONF_KEY = "hbase.regionserver.thrift.framed";
  static final String MAX_FRAME_SIZE_CONF_KEY = "hbase.regionserver.thrift.framed.max_frame_size_in_mb";
  static final String PORT_CONF_KEY = "hbase.regionserver.thrift.port";
  static final String COALESCE_INC_KEY = "hbase.regionserver.thrift.coalesceIncrement";
  static final String USE_HTTP_CONF_KEY = "hbase.regionserver.thrift.http";
  static final String HTTP_MIN_THREADS = "hbase.thrift.http_threads.min";
  static final String HTTP_MAX_THREADS = "hbase.thrift.http_threads.max";

  static final String THRIFT_SSL_ENABLED = "hbase.thrift.ssl.enabled";
  static final String THRIFT_SSL_KEYSTORE_STORE = "hbase.thrift.ssl.keystore.store";
  static final String THRIFT_SSL_KEYSTORE_PASSWORD = "hbase.thrift.ssl.keystore.password";
  static final String THRIFT_SSL_KEYSTORE_KEYPASSWORD = "hbase.thrift.ssl.keystore.keypassword";


  /**
   * Thrift quality of protection configuration key. Valid values can be:
   * auth-conf: authentication, integrity and confidentiality checking
   * auth-int: authentication and integrity checking
   * auth: authentication only
   *
   * This is used to authenticate the callers and support impersonation.
   * The thrift server and the HBase cluster must run in secure mode.
   */
  static final String THRIFT_QOP_KEY = "hbase.thrift.security.qop";

  private static final String DEFAULT_BIND_ADDR = "0.0.0.0";
  public static final int DEFAULT_LISTEN_PORT = 9090;
  public static final int HREGION_VERSION = 1;
  static final String THRIFT_SUPPORT_PROXYUSER = "hbase.thrift.support.proxyuser";
  private final int listenPort;

  private Configuration conf;
  volatile TServer tserver;
  volatile Server httpServer;
  private final Hbase.Iface handler;
  private final ThriftMetrics metrics;
  private final HBaseHandler hbaseHandler;
  private final UserGroupInformation realUser;

  private final String qop;
  private String host;

  private final boolean securityEnabled;
  private final boolean doAsEnabled;

  /**
   * ChoreService used to schedule tasks that we want to run periodically
   */
  private static ChoreService choreService;

  /** An enum of server implementation selections */
  enum ImplType {
    HS_HA("hsha", true, THsHaServer.class, true),
    NONBLOCKING("nonblocking", true, TNonblockingServer.class, true),
    THREAD_POOL("threadpool", false, TBoundedThreadPoolServer.class, true),
    THREADED_SELECTOR(
        "threadedselector", true, TThreadedSelectorServer.class, true);

    public static final ImplType DEFAULT = THREAD_POOL;

    final String option;
    final boolean isAlwaysFramed;
    final Class<? extends TServer> serverClass;
    final boolean canSpecifyBindIP;

    ImplType(String option, boolean isAlwaysFramed,
        Class<? extends TServer> serverClass, boolean canSpecifyBindIP) {
      this.option = option;
      this.isAlwaysFramed = isAlwaysFramed;
      this.serverClass = serverClass;
      this.canSpecifyBindIP = canSpecifyBindIP;
    }

    /**
     * @return <code>-option</code> so we can get the list of options from
     *         {@link #values()}
     */
    @Override
    public String toString() {
      return "-" + option;
    }

    String getDescription() {
      StringBuilder sb = new StringBuilder("Use the " +
          serverClass.getSimpleName());
      if (isAlwaysFramed) {
        sb.append(" This implies the framed transport.");
      }
      if (this == DEFAULT) {
        sb.append("This is the default.");
      }
      return sb.toString();
    }

    static OptionGroup createOptionGroup() {
      OptionGroup group = new OptionGroup();
      for (ImplType t : values()) {
        group.addOption(new Option(t.option, t.getDescription()));
      }
      return group;
    }

    static ImplType getServerImpl(Configuration conf) {
      String confType = conf.get(SERVER_TYPE_CONF_KEY, THREAD_POOL.option);
      for (ImplType t : values()) {
        if (confType.equals(t.option)) {
          return t;
        }
      }
      throw new AssertionError("Unknown server ImplType.option:" + confType);
    }

    static void setServerImpl(CommandLine cmd, Configuration conf) {
      ImplType chosenType = null;
      int numChosen = 0;
      for (ImplType t : values()) {
        if (cmd.hasOption(t.option)) {
          chosenType = t;
          ++numChosen;
        }
      }
      if (numChosen < 1) {
        LOG.info("Using default thrift server type");
        chosenType = DEFAULT;
      } else if (numChosen > 1) {
        throw new AssertionError("Exactly one option out of " +
          Arrays.toString(values()) + " has to be specified");
      }
      LOG.info("Using thrift server type " + chosenType.option);
      conf.set(SERVER_TYPE_CONF_KEY, chosenType.option);
    }

    public String simpleClassName() {
      return serverClass.getSimpleName();
    }

    public static List<String> serversThatCannotSpecifyBindIP() {
      List<String> l = new ArrayList<String>();
      for (ImplType t : values()) {
        if (!t.canSpecifyBindIP) {
          l.add(t.simpleClassName());
        }
      }
      return l;
    }

  }

  public ThriftServerRunner(Configuration conf) throws IOException {
    UserProvider userProvider = UserProvider.instantiate(conf);
    // login the server principal (if using secure Hadoop)
    securityEnabled = userProvider.isHadoopSecurityEnabled()
      && userProvider.isHBaseSecurityEnabled();
    if (securityEnabled) {
      host = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
        conf.get("hbase.thrift.dns.interface", "default"),
        conf.get("hbase.thrift.dns.nameserver", "default")));
      userProvider.login("hbase.thrift.keytab.file",
        "hbase.thrift.kerberos.principal", host);
    }
    this.conf = HBaseConfiguration.create(conf);
    this.listenPort = conf.getInt(PORT_CONF_KEY, DEFAULT_LISTEN_PORT);
    this.metrics = new ThriftMetrics(conf, ThriftMetrics.ThriftServerType.ONE);
    this.hbaseHandler = new HBaseHandler(conf, userProvider);
    this.hbaseHandler.initMetrics(metrics);
    this.handler = HbaseHandlerMetricsProxy.newInstance(
      hbaseHandler, metrics, conf);
    this.realUser = userProvider.getCurrent().getUGI();
    qop = conf.get(THRIFT_QOP_KEY);
    doAsEnabled = conf.getBoolean(THRIFT_SUPPORT_PROXYUSER, false);
    if (qop != null) {
      if (!qop.equals("auth") && !qop.equals("auth-int")
          && !qop.equals("auth-conf")) {
        throw new IOException("Invalid " + THRIFT_QOP_KEY + ": " + qop
          + ", it must be 'auth', 'auth-int', or 'auth-conf'");
      }
      if (!securityEnabled) {
        throw new IOException("Thrift server must"
          + " run in secure mode to support authentication");
      }
    }

    this.choreService = new ChoreService("thrift-chore-service");
  }

  /*
   * Runs the Thrift server
   */
  @Override
  public void run() {
    realUser.doAs(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        try {
          if (conf.getBoolean(USE_HTTP_CONF_KEY, false)) {
            setupHTTPServer();
            httpServer.start();
            httpServer.join();
          } else {
            setupServer();
            tserver.serve();
          }
        } catch (Exception e) {
          LOG.fatal("Cannot run ThriftServer", e);
          // Crash the process if the ThriftServer is not running
          System.exit(-1);
        }
        return null;
      }
    });
  }

  public void shutdown() {
    STOPPABLE.stop("Shutting down");
    if (tserver != null) {
      tserver.stop();
      tserver = null;
    }
    if (httpServer != null) {
      try {
        httpServer.stop();
        httpServer = null;
      } catch (Exception e) {
        LOG.error("Problem encountered in shutting down HTTP server " + e.getCause());
      }
      httpServer = null;
    }

    if(null != this.choreService) {
      this.choreService.shutdown();
    }
  }

  private void setupHTTPServer() throws IOException {
    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    TProcessor processor = new Hbase.Processor<Hbase.Iface>(handler);
    TServlet thriftHttpServlet = new ThriftHttpServlet(processor, protocolFactory, realUser,
        conf, hbaseHandler, securityEnabled, doAsEnabled);

    httpServer = new Server();
    // Context handler
    Context context = new Context(httpServer, "/", Context.SESSIONS);
    context.setContextPath("/");
    String httpPath = "/*";
    httpServer.setHandler(context);
    context.addServlet(new ServletHolder(thriftHttpServlet), httpPath);

    // set up Jetty and run the embedded server
    Connector connector = new SelectChannelConnector();
    if(conf.getBoolean(THRIFT_SSL_ENABLED, false)) {
      SslSelectChannelConnector sslConnector = new SslSelectChannelConnector();
      String keystore = conf.get(THRIFT_SSL_KEYSTORE_STORE);
      String password = HBaseConfiguration.getPassword(conf,
          THRIFT_SSL_KEYSTORE_PASSWORD, null);
      String keyPassword = HBaseConfiguration.getPassword(conf,
          THRIFT_SSL_KEYSTORE_KEYPASSWORD, password);
      sslConnector.setKeystore(keystore);
      sslConnector.setPassword(password);
      sslConnector.setKeyPassword(keyPassword);
      connector = sslConnector;
    }
    String host = getBindAddress(conf).getHostAddress();
    connector.setPort(listenPort);
    connector.setHost(host);
    httpServer.addConnector(connector);

    if (doAsEnabled) {
      ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    }

    // Set the default max thread number to 100 to limit
    // the number of concurrent requests so that Thrfit HTTP server doesn't OOM easily.
    // Jetty set the default max thread number to 250, if we don't set it.
    //
    // Our default min thread number 2 is the same as that used by Jetty.
    int minThreads = conf.getInt(HTTP_MIN_THREADS, 2);
    int maxThreads = conf.getInt(HTTP_MAX_THREADS, 100);
    QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads);
    threadPool.setMinThreads(minThreads);
    httpServer.setThreadPool(threadPool);

    httpServer.setSendServerVersion(false);
    httpServer.setSendDateHeader(false);
    httpServer.setStopAtShutdown(true);

    LOG.info("Starting Thrift HTTP Server on " + Integer.toString(listenPort));
  }

  /**
   * Setting up the thrift TServer
   */
  private void setupServer() throws Exception {
    // Construct correct ProtocolFactory
    TProtocolFactory protocolFactory;
    if (conf.getBoolean(COMPACT_CONF_KEY, false)) {
      LOG.debug("Using compact protocol");
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      LOG.debug("Using binary protocol");
      protocolFactory = new TBinaryProtocol.Factory(true, true);
    }

    final TProcessor p = new Hbase.Processor<Hbase.Iface>(handler);
    ImplType implType = ImplType.getServerImpl(conf);
    TProcessor processor = p;

    // Construct correct TransportFactory
    TTransportFactory transportFactory;
    if (conf.getBoolean(FRAMED_CONF_KEY, false) || implType.isAlwaysFramed) {
      if (qop != null) {
        throw new RuntimeException("Thrift server authentication"
          + " doesn't work with framed transport yet");
      }
      transportFactory = new TFramedTransport.Factory(
          conf.getInt(MAX_FRAME_SIZE_CONF_KEY, 2)  * 1024 * 1024);
      LOG.debug("Using framed transport");
    } else if (qop == null) {
      transportFactory = new TTransportFactory();
    } else {
      // Extract the name from the principal
      String name = SecurityUtil.getUserFromPrincipal(
        conf.get("hbase.thrift.kerberos.principal"));
      Map<String, String> saslProperties = new HashMap<String, String>();
      saslProperties.put(Sasl.QOP, qop);
      TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
      saslFactory.addServerDefinition("GSSAPI", name, host, saslProperties,
        new SaslGssCallbackHandler() {
          @Override
          public void handle(Callback[] callbacks)
              throws UnsupportedCallbackException {
            AuthorizeCallback ac = null;
            for (Callback callback : callbacks) {
              if (callback instanceof AuthorizeCallback) {
                ac = (AuthorizeCallback) callback;
              } else {
                throw new UnsupportedCallbackException(callback,
                    "Unrecognized SASL GSSAPI Callback");
              }
            }
            if (ac != null) {
              String authid = ac.getAuthenticationID();
              String authzid = ac.getAuthorizationID();
              if (!authid.equals(authzid)) {
                ac.setAuthorized(false);
              } else {
                ac.setAuthorized(true);
                String userName = SecurityUtil.getUserFromPrincipal(authzid);
                LOG.info("Effective user: " + userName);
                ac.setAuthorizedID(userName);
              }
            }
          }
        });
      transportFactory = saslFactory;

      // Create a processor wrapper, to get the caller
      processor = new TProcessor() {
        @Override
        public boolean process(TProtocol inProt,
            TProtocol outProt) throws TException {
          TSaslServerTransport saslServerTransport =
            (TSaslServerTransport)inProt.getTransport();
          SaslServer saslServer = saslServerTransport.getSaslServer();
          String principal = saslServer.getAuthorizationID();
          hbaseHandler.setEffectiveUser(principal);
          return p.process(inProt, outProt);
        }
      };
    }

    if (conf.get(BIND_CONF_KEY) != null && !implType.canSpecifyBindIP) {
      LOG.error("Server types " + Joiner.on(", ").join(
          ImplType.serversThatCannotSpecifyBindIP()) + " don't support IP " +
          "address binding at the moment. See " +
          "https://issues.apache.org/jira/browse/HBASE-2155 for details.");
      throw new RuntimeException(
          "-" + BIND_CONF_KEY + " not supported with " + implType);
    }

    if (implType == ImplType.HS_HA || implType == ImplType.NONBLOCKING ||
        implType == ImplType.THREADED_SELECTOR) {

      InetAddress listenAddress = getBindAddress(conf);
      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(
          new InetSocketAddress(listenAddress, listenPort));

      if (implType == ImplType.NONBLOCKING) {
        TNonblockingServer.Args serverArgs =
            new TNonblockingServer.Args(serverTransport);
        serverArgs.processor(processor)
                  .transportFactory(transportFactory)
                  .protocolFactory(protocolFactory);
        tserver = new TNonblockingServer(serverArgs);
      } else if (implType == ImplType.HS_HA) {
        THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
        CallQueue callQueue =
            new CallQueue(new LinkedBlockingQueue<Call>(), metrics);
        ExecutorService executorService = createExecutor(
            callQueue, serverArgs.getWorkerThreads());
        serverArgs.executorService(executorService)
                  .processor(processor)
                  .transportFactory(transportFactory)
                  .protocolFactory(protocolFactory);
        tserver = new THsHaServer(serverArgs);
      } else { // THREADED_SELECTOR
        TThreadedSelectorServer.Args serverArgs =
            new HThreadedSelectorServerArgs(serverTransport, conf);
        CallQueue callQueue =
            new CallQueue(new LinkedBlockingQueue<Call>(), metrics);
        ExecutorService executorService = createExecutor(
            callQueue, serverArgs.getWorkerThreads());
        serverArgs.executorService(executorService)
                  .processor(processor)
                  .transportFactory(transportFactory)
                  .protocolFactory(protocolFactory);
        tserver = new TThreadedSelectorServer(serverArgs);
      }
      LOG.info("starting HBase " + implType.simpleClassName() +
          " server on " + Integer.toString(listenPort));
    } else if (implType == ImplType.THREAD_POOL) {
      // Thread pool server. Get the IP address to bind to.
      InetAddress listenAddress = getBindAddress(conf);

      TServerTransport serverTransport = new TServerSocket(
          new InetSocketAddress(listenAddress, listenPort));

      TBoundedThreadPoolServer.Args serverArgs =
          new TBoundedThreadPoolServer.Args(serverTransport, conf);
      serverArgs.processor(processor)
                .transportFactory(transportFactory)
                .protocolFactory(protocolFactory);
      LOG.info("starting " + ImplType.THREAD_POOL.simpleClassName() + " on "
          + listenAddress + ":" + Integer.toString(listenPort)
          + "; " + serverArgs);
      TBoundedThreadPoolServer tserver =
          new TBoundedThreadPoolServer(serverArgs, metrics);
      this.tserver = tserver;
    } else {
      throw new AssertionError("Unsupported Thrift server implementation: " +
          implType.simpleClassName());
    }

    // A sanity check that we instantiated the right type of server.
    if (tserver.getClass() != implType.serverClass) {
      throw new AssertionError("Expected to create Thrift server class " +
          implType.serverClass.getName() + " but got " +
          tserver.getClass().getName());
    }



    registerFilters(conf);
  }

  ExecutorService createExecutor(BlockingQueue<Runnable> callQueue,
                                 int workerThreads) {
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setDaemon(true);
    tfb.setNameFormat("thrift-worker-%d");
    return new ThreadPoolExecutor(workerThreads, workerThreads,
            Long.MAX_VALUE, TimeUnit.SECONDS, callQueue, tfb.build());
  }

  private InetAddress getBindAddress(Configuration conf)
      throws UnknownHostException {
    String bindAddressStr = conf.get(BIND_CONF_KEY, DEFAULT_BIND_ADDR);
    return InetAddress.getByName(bindAddressStr);
  }

  /**
   * The HBaseHandler is a glue object that connects Thrift RPC calls to the
   * HBase client API primarily defined in the Admin and Table objects.
   */
  public static class HBaseHandler implements Hbase.Iface {
    protected Configuration conf;
    protected final Log LOG = LogFactory.getLog(this.getClass().getName());

    // nextScannerId and scannerMap are used to manage scanner state
    protected int nextScannerId = 0;
    protected Map<Integer, HTableScanner> scannerMap = null;
    protected int nextMessageScannerId = 0;
    protected Map<Integer, HQueueMessageScanner> messageScannerMap = null;
    protected int nextPartitionScannerId = 0;
    protected Map<Integer, HQueuePartitionScanner> partitionScannerMap = null;
    protected int nextQueueScannerId = 0;
    protected Map<Integer, HQueueScanner> queueScannerMap = null;
    private ThriftMetrics metrics = null;

    private static ThreadLocal<Map<String, HQueue>> threadLocalQueues =
        new ThreadLocal<Map<String, HQueue>>() {
          @Override
          protected Map<String, HQueue> initialValue() {
            return new TreeMap<String, HQueue>();
          }
        };

    private final ConnectionCache connectionCache;
    IncrementCoalescer coalescer = null;

    static final String CLEANUP_INTERVAL = "hbase.thrift.connection.cleanup-interval";
    static final String MAX_IDLETIME = "hbase.thrift.connection.max-idletime";

    /**
     * Returns a list of all the column families for a given Table.
     *
     * @param table
     * @throws IOException
     */
    byte[][] getAllColumns(Table table) throws IOException {
      HColumnDescriptor[] cds = table.getTableDescriptor().getColumnFamilies();
      byte[][] columns = new byte[cds.length][];
      for (int i = 0; i < cds.length; i++) {
        columns[i] = Bytes.add(cds[i].getName(),
            KeyValue.COLUMN_FAMILY_DELIM_ARRAY);
      }
      return columns;
    }

    /**
     * Creates and returns a Table instance from a given table name.
     *
     * @param tableName
     *          name of table
     * @return Table object
     * @throws IOException
     * @throws IOError
     */
    public Table getTable(final byte[] tableName) throws
        IOException {
      String table = Bytes.toString(tableName);
      LOG.info("create table handler for [" + table + "] in " + Thread.currentThread().getName());
      return connectionCache.getTable(table);
    }

    public Table getTable(final ByteBuffer tableName) throws IOException {
      return getTable(getBytes(tableName));
    }

    /**
     * Creates and returns an HQueue instance from a given queue name.
     * @param queueName name of table
     * @return HQueue object
     * @throws IOException
     * @throws IOError
     */
    public HQueue getQueue(final byte[] queueName) throws IOException {
      String queue = Bytes.toString(queueName);
      Map<String, HQueue> queues = threadLocalQueues.get();
      if (!queues.containsKey(queue)) {
        queues.put(queue, new HQueue(conf, queue));
        LOG.info("create queue handler for [" + queue + "] in " + Thread.currentThread().getName());
      }
      return queues.get(queue);
    }

    public HQueue getQueue(final ByteBuffer queueName) throws IOException {
      return getQueue(getBytes(queueName));
    }

    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal
     * hash-map.
     *
     * @param scanner
     * @return integer scanner id
     */
    protected synchronized int addScanner(HTableScanner scanner) {
      int id = nextScannerId++;
      scannerMap.put(id, scanner);
      if (null != metrics) {
        metrics.setTableScannerNum(scannerMap.size());
      }

      return id;
    }

    /**
     * Returns the scanner associated with the specified ID.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized HTableScanner getScanner(int id) {
      return scannerMap.get(id);
    }

    /**
     * Removes the scanner associated with the specified ID from the internal
     * id->scanner hash-map.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized HTableScanner removeScanner(int id) {
      HTableScanner scanner = scannerMap.remove(id);
      if (null != metrics) {
        metrics.setTableScannerNum(scannerMap.size());
      }

      return scanner;
    }

    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal hash-map.
     * @param scanner
     * @return integer scanner id
     */
    protected synchronized int addMessageScanner(HQueueMessageScanner scanner) {
      int id = nextMessageScannerId++;
      messageScannerMap.put(id, scanner);
      if (null != metrics) {
        metrics.setMessageScannerNum(messageScannerMap.size());
      }

      return id;
    }

    /**
     * Returns the scanner associated with the specified ID.
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized HQueueMessageScanner getMessageScanner(int id) {
      return messageScannerMap.get(id);
    }

    /**
     * Removes the scanner associated with the specified ID from the internal id->scanner hash-map.
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized HQueueMessageScanner removeMessageScanner(int id) {
      HQueueMessageScanner scanner = messageScannerMap.remove(id);
      if (null != metrics) {
        metrics.setMessageScannerNum(messageScannerMap.size());
      }

      return scanner;
    }

    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal hash-map.
     * @param scanner
     * @return integer scanner id
     */
    protected synchronized int addPartitionScanner(HQueuePartitionScanner scanner) {
      int id = nextPartitionScannerId++;
      partitionScannerMap.put(id, scanner);
      if (null != metrics) {
        metrics.setPartitionScannerNum(partitionScannerMap.size());
      }

      return id;
    }

    /**
     * Returns the scanner associated with the specified ID.
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized HQueuePartitionScanner getPartitionScanner(int id) {
      return partitionScannerMap.get(id);
    }

    /**
     * Removes the scanner associated with the specified ID from the internal id->scanner hash-map.
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized HQueuePartitionScanner removePartitionScanner(int id) {
      HQueuePartitionScanner scanner = partitionScannerMap.remove(id);
      if (null != metrics) {
        metrics.setPartitionScannerNum(partitionScannerMap.size());
      }

      return scanner;
    }

    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal hash-map.
     * @param scanner
     * @return integer scanner id
     */
    protected synchronized int addQueueScanner(HQueueScanner scanner) {
      int id = nextQueueScannerId++;
      queueScannerMap.put(id, scanner);
      if (null != metrics) {
        metrics.setQueueScannerNum(queueScannerMap.size());
      }

      return id;
    }

    /**
     * Returns the scanner associated with the specified ID.
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized HQueueScanner getQueueScanner(int id) {
      return queueScannerMap.get(id);
    }

    /**
     * Removes the scanner associated with the specified ID from the internal id->scanner hash-map.
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized HQueueScanner removeQueueScanner(int id) {
      HQueueScanner scanner = queueScannerMap.remove(id);
      if (null != metrics) {
        metrics.setQueueScannerNum(queueScannerMap.size());
      }

      return scanner;
    }

    protected HBaseHandler(final Configuration c,
        final UserProvider userProvider) throws IOException {
      this.conf = c;
      this.coalescer = new IncrementCoalescer(this);

      scannerMap = new ConcurrentHashMap<Integer, HTableScanner>();
      messageScannerMap = new ConcurrentHashMap<Integer, HQueueMessageScanner>();
      partitionScannerMap = new ConcurrentHashMap<Integer, HQueuePartitionScanner>();
      queueScannerMap = new ConcurrentHashMap<Integer, HQueueScanner>();

      int scannerTimeout = this.conf.getInt("hbase.thrift.scanner.cleaner.timeout", 1000 * 60 * 10);
      int scannerCleanerInterval =
          this.conf.getInt("hbase.thrift.scanner.cleaner.interval", 1000 * 60 * 3);
      ScannerCleanerChore cleaner =
          new ScannerCleanerChore("Scanner cleaner", scannerTimeout, scannerCleanerInterval, STOPPABLE);
      if (null != choreService) {
        choreService.scheduleChore(cleaner);
      }

      int cleanInterval = conf.getInt(CLEANUP_INTERVAL, 10 * 1000);
      int maxIdleTime = conf.getInt(MAX_IDLETIME, 10 * 60 * 1000);
      connectionCache = new ConnectionCache(
        conf, userProvider, cleanInterval, maxIdleTime);
    }

    /**
     * Obtain HBaseAdmin. Creates the instance if it is not already created.
     */
    private Admin getAdmin() throws IOException {
      return connectionCache.getAdmin();
    }

    void setEffectiveUser(String effectiveUser) {
      connectionCache.setEffectiveUser(effectiveUser);
    }

    @Override
    public void enableTable(ByteBuffer tableName) throws IOError {
      try{
        getAdmin().enableTable(getTableName(tableName));
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    @Override
    public void disableTable(ByteBuffer tableName) throws IOError{
      try{
        getAdmin().disableTable(getTableName(tableName));
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    @Override
    public boolean isTableEnabled(ByteBuffer tableName) throws IOError {
      try {
        return this.connectionCache.getAdmin().isTableEnabled(getTableName(tableName));
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    @Override
    public void compact(ByteBuffer tableNameOrRegionName) throws IOError {
      try {
        // TODO: HBaseAdmin.compact(byte[]) deprecated and not trivial to replace here.
        // ThriftServerRunner.compact should be deprecated and replaced with methods specific to
        // table and region.
        ((HBaseAdmin) getAdmin()).compact(getBytes(tableNameOrRegionName));
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    @Override
    public void majorCompact(ByteBuffer tableNameOrRegionName) throws IOError {
      try {
        // TODO: HBaseAdmin.majorCompact(byte[]) deprecated and not trivial to replace here.
        // ThriftServerRunner.majorCompact should be deprecated and replaced with methods specific
        // to table and region.
        ((HBaseAdmin) getAdmin()).majorCompact(getBytes(tableNameOrRegionName));
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    @Override
    public List<ByteBuffer> getTableNames() throws IOError {
      try {
        TableName[] tableNames = this.getAdmin().listTableNames();
        ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>(tableNames.length);
        for (int i = 0; i < tableNames.length; i++) {
          list.add(ByteBuffer.wrap(tableNames[i].getName()));
        }
        return list;
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    /**
     * @return the list of regions in the given table, or an empty list if the table does not exist
     */
    @Override
    public List<TRegionInfo> getTableRegions(ByteBuffer tableName)
    throws IOError {
      try (RegionLocator locator = connectionCache.getRegionLocator(getBytes(tableName))) {
        List<HRegionLocation> regionLocations = locator.getAllRegionLocations();
        List<TRegionInfo> results = new ArrayList<TRegionInfo>();
        for (HRegionLocation regionLocation : regionLocations) {
          HRegionInfo info = regionLocation.getRegionInfo();
          ServerName serverName = regionLocation.getServerName();
          TRegionInfo region = new TRegionInfo();
          region.serverName = ByteBuffer.wrap(
              Bytes.toBytes(serverName.getHostname()));
          region.port = serverName.getPort();
          region.startKey = ByteBuffer.wrap(info.getStartKey());
          region.endKey = ByteBuffer.wrap(info.getEndKey());
          region.id = info.getRegionId();
          region.name = ByteBuffer.wrap(info.getRegionName());
          region.version = info.getVersion();
          results.add(region);
        }
        return results;
      } catch (TableNotFoundException e) {
        // Return empty list for non-existing table
        return Collections.emptyList();
      } catch (IOException e){
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    @Deprecated
    @Override
    public List<TCell> get(
        ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if (famAndQf.length == 1) {
        return get(tableName, row, famAndQf[0], null, attributes);
      }
      if (famAndQf.length == 2) {
        return get(tableName, row, famAndQf[0], famAndQf[1], attributes);
      }
      throw new IllegalArgumentException("Invalid familyAndQualifier provided.");
    }

    /**
     * Note: this internal interface is slightly different from public APIs in regard to handling
     * of the qualifier. Here we differ from the public Java API in that null != byte[0]. Rather,
     * we respect qual == null as a request for the entire column family. The caller (
     * {@link #get(ByteBuffer, ByteBuffer, ByteBuffer, Map)}) interface IS consistent in that the
     * column is parse like normal.
     */
    protected List<TCell> get(ByteBuffer tableName,
                              ByteBuffer row,
                              byte[] family,
                              byte[] qualifier,
                              Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      Table table = null;
      try {
        table = getTable(tableName);
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        if (qualifier == null) {
          get.addFamily(family);
        } else {
          get.addColumn(family, qualifier);
        }
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.rawCells());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally {
        closeTable(table);
      }
    }

    @Deprecated
    @Override
    public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        int numVersions, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return getVer(tableName, row, famAndQf[0], null, numVersions, attributes);
      }
      if (famAndQf.length == 2) {
        return getVer(tableName, row, famAndQf[0], famAndQf[1], numVersions, attributes);
      }
      throw new IllegalArgumentException("Invalid familyAndQualifier provided.");

    }

    /**
     * Note: this public interface is slightly different from public Java APIs in regard to
     * handling of the qualifier. Here we differ from the public Java API in that null != byte[0].
     * Rather, we respect qual == null as a request for the entire column family. If you want to
     * access the entire column family, use
     * {@link #getVer(ByteBuffer, ByteBuffer, ByteBuffer, int, Map)} with a {@code column} value
     * that lacks a {@code ':'}.
     */
    public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row, byte[] family,
        byte[] qualifier, int numVersions, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      
      Table table = null;
      try {
        table = getTable(tableName);
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        if (null == qualifier) {
          get.addFamily(family);
        } else {
          get.addColumn(family, qualifier);
        }
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.rawCells());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Deprecated
    @Override
    public List<TCell> getVerTs(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        long timestamp, int numVersions, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if (famAndQf.length == 1) {
        return getVerTs(tableName, row, famAndQf[0], null, timestamp, numVersions, attributes);
      }
      if (famAndQf.length == 2) {
        return getVerTs(tableName, row, famAndQf[0], famAndQf[1], timestamp, numVersions,
          attributes);
      }
      throw new IllegalArgumentException("Invalid familyAndQualifier provided.");
    }

    /**
     * Note: this internal interface is slightly different from public APIs in regard to handling
     * of the qualifier. Here we differ from the public Java API in that null != byte[0]. Rather,
     * we respect qual == null as a request for the entire column family. The caller (
     * {@link #getVerTs(ByteBuffer, ByteBuffer, ByteBuffer, long, int, Map)}) interface IS
     * consistent in that the column is parse like normal.
     */
    protected List<TCell> getVerTs(ByteBuffer tableName, ByteBuffer row, byte[] family,
        byte[] qualifier, long timestamp, int numVersions, Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      
      Table table = null;
      try {
        table = getTable(tableName);
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        if (null == qualifier) {
          get.addFamily(family);
        } else {
          get.addColumn(family, qualifier);
        }
        get.setTimeRange(0, timestamp);
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.rawCells());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public List<TRowResult> getRow(ByteBuffer tableName, ByteBuffer row,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 HConstants.LATEST_TIMESTAMP,
                                 attributes);
    }

    @Override
    public List<TRowResult> getRowVers(ByteBuffer tableName, ByteBuffer row,
        Map<ByteBuffer, ByteBuffer> attributes, int numVersions) throws IOError, TException {
      return getRowWithColumnsVers(tableName, row, null, numVersions, attributes);
    }

    @Override
    public List<TRowResult> getRowWithColumns(ByteBuffer tableName,
                                              ByteBuffer row,
        List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowWithColumnsTs(tableName, row, columns,
                                 HConstants.LATEST_TIMESTAMP,
                                 attributes);
    }

    @Override
    public List<TRowResult> getRowTs(ByteBuffer tableName, ByteBuffer row,
        long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 timestamp, attributes);
    }

    @Override
    public List<TRowResult> getRowWithColumnsTs(
        ByteBuffer tableName, ByteBuffer row, List<ByteBuffer> columns,
        long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      
      Table table = null;
      try {
        table = getTable(tableName);
        if (columns == null) {
          Get get = new Get(getBytes(row));
          addAttributes(get, attributes);
          get.setTimeRange(0, timestamp);
          Result result = table.get(get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        for(ByteBuffer column : columns) {
          byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
          if (famAndQf.length == 1) {
              get.addFamily(famAndQf[0]);
          } else {
              get.addColumn(famAndQf[0], famAndQf[1]);
          }
        }
        get.setTimeRange(0, timestamp);
        Result result = table.get(get);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    public List<TRowResult> getRowWithColumnsVers(ByteBuffer tableName, ByteBuffer row,
        List<ByteBuffer> columns, int maxVersions, Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      Table table = null;
      try {
        table = getTable(tableName);
        if (columns == null) {
          Get get = new Get(getBytes(row));
          addAttributes(get, attributes);
          get.setMaxVersions(maxVersions);
          Result result = table.get(get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        for (ByteBuffer column : columns) {
          byte[][] famAndQf = KeyValue.parseColumn(getBytes(column));
          if (famAndQf.length == 1) {
            get.addFamily(famAndQf[0]);
          } else {
            get.addColumn(famAndQf[0], famAndQf[1]);
          }
        }
        get.setMaxVersions(maxVersions);
        Result result = table.get(get);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public List<TRowResult> getRows(ByteBuffer tableName,
                                    List<ByteBuffer> rows,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  HConstants.LATEST_TIMESTAMP,
                                  attributes);
    }

    @Override
    public List<TRowResult> getRowsWithColumns(ByteBuffer tableName,
                                               List<ByteBuffer> rows,
        List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, columns,
                                  HConstants.LATEST_TIMESTAMP,
                                  attributes);
    }

    @Override
    public List<TRowResult> getRowsTs(ByteBuffer tableName,
                                      List<ByteBuffer> rows,
        long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  timestamp, attributes);
    }

    @Override
    public List<TRowResult> getRowsWithColumnsTs(ByteBuffer tableName,
                                                 List<ByteBuffer> rows,
        List<ByteBuffer> columns, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      
      Table table= null;
      try {
        List<Get> gets = new ArrayList<Get>(rows.size());
        table = getTable(tableName);
        if (metrics != null) {
          metrics.incNumRowKeysInBatchGet(rows.size());
        }
        for (ByteBuffer row : rows) {
          Get get = new Get(getBytes(row));
          addAttributes(get, attributes);
          if (columns != null) {

            for(ByteBuffer column : columns) {
              byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
              if (famAndQf.length == 1) {
                get.addFamily(famAndQf[0]);
              } else {
                get.addColumn(famAndQf[0], famAndQf[1]);
              }
            }
          }
          get.setTimeRange(0, timestamp);
          gets.add(get);
        }
        Result[] result = table.get(gets);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public void deleteAll(
        ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      deleteAllTs(tableName, row, column, HConstants.LATEST_TIMESTAMP,
                  attributes);
    }

    @Override
    public void deleteAllTs(ByteBuffer tableName,
                            ByteBuffer row,
                            ByteBuffer column,
        long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      Table table = null;
      try {
        table = getTable(tableName);
        Delete delete  = new Delete(getBytes(row));
        addAttributes(delete, attributes);
        byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
        if (famAndQf.length == 1) {
          delete.deleteFamily(famAndQf[0], timestamp);
        } else {
          delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
        }
        table.delete(delete);

      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally {
        closeTable(table);
      }
    }

    @Override
    public void deleteAllRow(
        ByteBuffer tableName, ByteBuffer row,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      deleteAllRowTs(tableName, row, HConstants.LATEST_TIMESTAMP, attributes);
    }

    @Override
    public void deleteAllRowTs(
        ByteBuffer tableName, ByteBuffer row, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      Table table = null;
      try {
        table = getTable(tableName);
        Delete delete  = new Delete(getBytes(row), timestamp);
        addAttributes(delete, attributes);
        table.delete(delete);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally {
        closeTable(table);
      }
    }

    @Override
    public void createTable(ByteBuffer in_tableName,
        List<ColumnDescriptor> columnFamilies) throws IOError,
        IllegalArgument, AlreadyExists {
      TableName tableName = getTableName(in_tableName);
      try {
        if (getAdmin().tableExists(tableName)) {
          throw new AlreadyExists("table name already in use");
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (ColumnDescriptor col : columnFamilies) {
          HColumnDescriptor colDesc = ThriftUtilities.colDescFromThrift(col);
          desc.addFamily(colDesc);
        }
        getAdmin().createTable(desc);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage(), e);
        throw new IllegalArgument(Throwables.getStackTraceAsString(e));
      }
    }

    private static TableName getTableName(ByteBuffer buffer) {
      return TableName.valueOf(getBytes(buffer));
    }

    @Override
    public void deleteTable(ByteBuffer in_tableName) throws IOError {
      TableName tableName = getTableName(in_tableName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("deleteTable: table=" + tableName);
      }
      try {
        if (!getAdmin().tableExists(tableName)) {
          throw new IOException("table does not exist");
        }
        getAdmin().deleteTable(tableName);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    @Override
    public void mutateRow(ByteBuffer tableName, ByteBuffer row,
        List<Mutation> mutations, Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, IllegalArgument {
      mutateRowTs(tableName, row, mutations, HConstants.LATEST_TIMESTAMP,
                  attributes);
    }

    @Override
    public void mutateRowTs(ByteBuffer tableName, ByteBuffer row,
        List<Mutation> mutations, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, IllegalArgument {
      Table table = null;
      try {
        table = getTable(tableName);
        Put put = new Put(getBytes(row), timestamp);
        addAttributes(put, attributes);

        Delete delete = new Delete(getBytes(row));
        addAttributes(delete, attributes);
        if (metrics != null) {
          metrics.incNumRowKeysInBatchMutate(mutations.size());
        }

        // I apologize for all this mess :)
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
          if (m.isDelete) {
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
            delete.setDurability(m.writeToWAL ? Durability.USE_DEFAULT
                : Durability.SKIP_WAL);
          } else {
            if(famAndQf.length == 1) {
              LOG.warn("No column qualifier specified. Delete is the only mutation supported "
                  + "over the whole column family.");
            } else {
              put.addImmutable(famAndQf[0], famAndQf[1],
                  m.value != null ? getBytes(m.value)
                      : HConstants.EMPTY_BYTE_ARRAY);
            }
            put.setDurability(m.writeToWAL ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
          }
        }
        if (!delete.isEmpty())
          table.delete(delete);
        if (!put.isEmpty())
          table.put(put);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage(), e);
        throw new IllegalArgument(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public void mutateRows(ByteBuffer tableName, List<BatchMutation> rowBatches,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, IllegalArgument, TException {
      mutateRowsTs(tableName, rowBatches, HConstants.LATEST_TIMESTAMP, attributes);
    }

    @Override
    public void mutateRowsTs(
        ByteBuffer tableName, List<BatchMutation> rowBatches, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, IllegalArgument, TException {
      List<Put> puts = new ArrayList<Put>();
      List<Delete> deletes = new ArrayList<Delete>();

      for (BatchMutation batch : rowBatches) {
        byte[] row = getBytes(batch.row);
        List<Mutation> mutations = batch.mutations;
        Delete delete = new Delete(row);
        addAttributes(delete, attributes);
        Put put = new Put(row, timestamp);
        addAttributes(put, attributes);
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
          if (m.isDelete) {
            // no qualifier, family only.
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
            delete.setDurability(m.writeToWAL ? Durability.USE_DEFAULT
                : Durability.SKIP_WAL);
          } else {
            if (famAndQf.length == 1) {
              LOG.warn("No column qualifier specified. Delete is the only mutation supported "
                  + "over the whole column family.");
            }
            if (famAndQf.length == 2) {
              put.addImmutable(famAndQf[0], famAndQf[1],
                  m.value != null ? getBytes(m.value)
                      : HConstants.EMPTY_BYTE_ARRAY);
            } else {
              throw new IllegalArgumentException("Invalid famAndQf provided.");
            }
            put.setDurability(m.writeToWAL ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
          }
        }
        if (!delete.isEmpty())
          deletes.add(delete);
        if (!put.isEmpty())
          puts.add(put);
      }

      Table table = null;
      try {
        table = getTable(tableName);
        if (!puts.isEmpty())
          table.put(puts);
        if (!deletes.isEmpty())
          table.delete(deletes);

      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage(), e);
        throw new IllegalArgument(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Deprecated
    @Override
    public long atomicIncrement(
        ByteBuffer tableName, ByteBuffer row, ByteBuffer column, long amount)
            throws IOError, IllegalArgument, TException {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return atomicIncrement(tableName, row, famAndQf[0], HConstants.EMPTY_BYTE_ARRAY, amount);
      }
      return atomicIncrement(tableName, row, famAndQf[0], famAndQf[1], amount);
    }

    protected long atomicIncrement(ByteBuffer tableName, ByteBuffer row,
        byte [] family, byte [] qualifier, long amount)
        throws IOError, IllegalArgument, TException {
      Table table = null;
      try {
        table = getTable(tableName);
        return table.incrementColumnValue(
            getBytes(row), family, qualifier, amount);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally {
        closeTable(table);
      }
    }

    @Override
    public void scannerClose(int id) throws IOError, IllegalArgument {
      LOG.debug("scannerClose: id=" + id);
      HTableScanner scanner = removeScanner(id);
      if (scanner == null) {
        String message = "CLOSE table scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("CLOSE table scanner ID is invalid");
      }

      scanner.getInternalScanner().close();
    }

    @Override
    public List<TRowResult> scannerGetList(int id,int nbRows)
        throws IllegalArgument, IOError {
      HTableScanner scanner = getScanner(id);
      if (null == scanner) {
        String message = "GET table scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("GET table scanner ID is invalid");
      }

      Result [] results = null;
      try {
        results = scanner.getInternalScanner().next(nbRows);
        if (null == results) {
          scanner.getInternalScanner().close();
          removeScanner(id);
          return new ArrayList<TRowResult>();
        }
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }

      scanner.updateTime();
      return ThriftUtilities.rowResultFromHBase(results, scanner.isColumnSorted());
    }

    @Override
    public List<TRowResult> scannerGet(int id) throws IllegalArgument, IOError {
      return scannerGetList(id,1);
    }

    @Override
    public int scannerOpenWithScan(ByteBuffer tableName, TScan tScan,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError {
      
      Table table = null;
      try {
        table = getTable(tableName);
        Scan scan = new Scan();
        addAttributes(scan, attributes);
        if (tScan.isSetStartRow()) {
          scan.setStartRow(tScan.getStartRow());
        }
        if (tScan.isSetStopRow()) {
          scan.setStopRow(tScan.getStopRow());
        }
        if (tScan.isSetTimestamp()) {
          scan.setTimeRange(0, tScan.getTimestamp());
        }
        if (tScan.isSetCaching()) {
          scan.setCaching(tScan.getCaching());
        }
        if (tScan.isSetBatchSize()) {
          scan.setBatch(tScan.getBatchSize());
        }
        if (tScan.isSetColumns() && tScan.getColumns().size() != 0) {
          for(ByteBuffer column : tScan.getColumns()) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        if (tScan.isSetFilterString()) {
          ParseFilter parseFilter = new ParseFilter();
          scan.setFilter(
              parseFilter.parseFilterString(tScan.getFilterString()));
        }
        if (tScan.isSetReversed()) {
          scan.setReversed(tScan.isReversed());
        }
        HTableScanner scanner =
            new HTableScanner(table.getScanner(scan), System.currentTimeMillis(),
                Bytes.toString(table.getTableDescriptor().getTableName().getName()), tScan.sortColumns);
        return addScanner(scanner);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public int scannerOpen(ByteBuffer tableName, ByteBuffer startRow,
        List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
      
      Table table = null;
      try {
        table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow));
        addAttributes(scan, attributes);
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        HTableScanner scanner =
            new HTableScanner(table.getScanner(scan), System.currentTimeMillis(),
                Bytes.toString(table.getTableDescriptor().getTableName().getName()), false);
        return addScanner(scanner);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public int scannerOpenWithStop(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, TException {
      
      Table table = null;
      try {
        table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        addAttributes(scan, attributes);
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        HTableScanner scanner =
            new HTableScanner(table.getScanner(scan), System.currentTimeMillis(),
                Bytes.toString(table.getTableDescriptor().getTableName().getName()), false);
        return addScanner(scanner);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public int scannerOpenWithPrefix(ByteBuffer tableName,
                                     ByteBuffer startAndPrefix,
                                     List<ByteBuffer> columns,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, TException {
      
      Table table = null;
      try {
        table = getTable(tableName);
        Scan scan = new Scan(getBytes(startAndPrefix));
        addAttributes(scan, attributes);
        Filter f = new WhileMatchFilter(
            new PrefixFilter(getBytes(startAndPrefix)));
        scan.setFilter(f);
        if (columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        HTableScanner scanner =
            new HTableScanner(table.getScanner(scan), System.currentTimeMillis(),
                Bytes.toString(table.getTableDescriptor().getTableName().getName()), false);
        return addScanner(scanner);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public int scannerOpenTs(ByteBuffer tableName, ByteBuffer startRow,
        List<ByteBuffer> columns, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes) throws IOError, TException {
      
      Table table = null;
      try {
        table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow));
        addAttributes(scan, attributes);
        scan.setTimeRange(0, timestamp);
        if (columns != null && columns.size() != 0) {
          for (ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        HTableScanner scanner =
            new HTableScanner(table.getScanner(scan), System.currentTimeMillis(),
                Bytes.toString(table.getTableDescriptor().getTableName().getName()), false);
        return addScanner(scanner);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public int scannerOpenWithStopTs(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, List<ByteBuffer> columns, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, TException {
      
      Table table = null;
      try {
        table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        addAttributes(scan, attributes);
        scan.setTimeRange(0, timestamp);
        if (columns != null && columns.size() != 0) {
          for (ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        scan.setTimeRange(0, timestamp);
        HTableScanner scanner =
            new HTableScanner(table.getScanner(scan), System.currentTimeMillis(),
                Bytes.toString(table.getTableDescriptor().getTableName().getName()), false);
        return addScanner(scanner);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public int scannerOpenWithTimeRange(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, List<ByteBuffer> columns, long startTime, long endTime) throws IOError,
        TException {
      Table table = null;
      try {
        table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        scan.setTimeRange(startTime, endTime);
        scan.setBatch(conf.getInt("hbase.client.scanner.batch", -1));
        if (columns != null && columns.size() != 0) {
          for (ByteBuffer column : columns) {
            byte[][] famQf = KeyValue.parseColumn(getBytes(column));
            if (famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }

        HTableScanner scanner =
            new HTableScanner(table.getScanner(scan), System.currentTimeMillis(),
                Bytes.toString(table.getTableDescriptor().getTableName().getName()), false);
        return addScanner(scanner);
      } catch (IOException e) {
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public int
    messageScannerOpen(ByteBuffer queueName, short partitionID, TMessageScan messageScan)
        throws IOError, TException {
      try {
        HQueue queue = getQueue(queueName);
        TMessageID startTMessageID = messageScan.getStartMessageID();
        TMessageID stopTMessageID = messageScan.getStopMessageID();
        List<ByteBuffer> topics = messageScan.getTopics();
        MessageID startMessageID =
            new MessageID(startTMessageID.getTimestamp(), startTMessageID.getSequenceID());
        MessageID stopMessageID =
            new MessageID(stopTMessageID.getTimestamp(), stopTMessageID.getSequenceID());
        com.etao.hadoop.hbase.queue.client.Scan scan =
            new com.etao.hadoop.hbase.queue.client.Scan(startMessageID, stopMessageID);
        if (null != topics) {
          for (ByteBuffer topic : topics) {
            scan.addTopic(getBytes(topic));
          }
        }

        HQueueMessageScanner scanner =
            new HQueueMessageScanner(queue.getScanner(partitionID, scan),
                System.currentTimeMillis(), queue.getQueueName());
        return addMessageScanner(scanner);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public TMessage messageScannerGet(int id) throws IOError, IllegalArgument, TException {
      List<TMessage> tMessages = messageScannerGetList(id, 1);
      if (tMessages.isEmpty()) {
        return null;
      } else {
        return tMessages.get(0);
      }
    }

    @Override
    public List<TMessage> messageScannerGetList(int id, int nbMessages) throws IOError,
        IllegalArgument, TException {
      HQueueMessageScanner scanner = getMessageScanner(id);
      if (null == scanner) {
        String message = "GET message scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("GET message scanner ID is invalid");
      }

      Message[] messages = null;
      try {
        messages = scanner.getInternalScanner().next(nbMessages);
        if (null == messages) {
          scanner.getInternalScanner().close();
          removeMessageScanner(id);
          return new ArrayList<TMessage>();
        }
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }

      scanner.updateTime();
      return ThriftUtilities.messageFromHBase(messages);
    }

    @Override
    public int partitionScannerOpen(ByteBuffer queueName, short partitionID,
        TMessageScan messageScan) throws IOError, TException {
      try {
        HQueue queue = getQueue(queueName);
        TMessageID startTMessageID = messageScan.getStartMessageID();
        TMessageID stopTMessageID = messageScan.getStopMessageID();
        List<ByteBuffer> topics = messageScan.getTopics();
        MessageID startMessageID =
            new MessageID(startTMessageID.getTimestamp(), startTMessageID.getSequenceID());
        MessageID stopMessageID =
            new MessageID(stopTMessageID.getTimestamp(), stopTMessageID.getSequenceID());
        com.etao.hadoop.hbase.queue.client.Scan scan =
            new com.etao.hadoop.hbase.queue.client.Scan(startMessageID, stopMessageID);
        if (null != topics) {
          for (ByteBuffer topic : topics) {
            scan.addTopic(getBytes(topic));
          }
        }

        HQueuePartitionScanner scanner =
            new HQueuePartitionScanner(queue.getPartitionScanner(partitionID, scan),
                System.currentTimeMillis(), queue.getQueueName());
        return addPartitionScanner(scanner);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public TMessage partitionScannerGet(int id) throws IOError, IllegalArgument, TException {
      List<TMessage> tMessages = partitionScannerGetList(id, 1);
      if (tMessages.isEmpty()) {
        return null;
      } else {
        return tMessages.get(0);
      }
    }

    @Override
    public List<TMessage> partitionScannerGetList(int id, int nbMessages) throws IOError,
        IllegalArgument, TException {
      HQueuePartitionScanner scanner = getPartitionScanner(id);
      if (null == scanner) {
        String message = "GET partition scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("GET partition scanner ID is invalid");
      }

      Message[] messages = null;
      try {
        messages = scanner.getInternalScanner().next(nbMessages);
        if (null == messages) {
          scanner.getInternalScanner().close();
          removePartitionScanner(id);
          return new ArrayList<TMessage>();
        }
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }

      scanner.updateTime();
      return ThriftUtilities.messageFromHBase(messages);
    }

    @Override
    public int queueScannerOpen(ByteBuffer queueName, TMessageScan messageScan) throws IOError,
        TException {
      try {
        HQueue queue = getQueue(queueName);
        TMessageID startTMessageID = messageScan.getStartMessageID();
        TMessageID stopTMessageID = messageScan.getStopMessageID();
        List<ByteBuffer> topics = messageScan.getTopics();
        MessageID startMessageID =
            new MessageID(startTMessageID.getTimestamp(), startTMessageID.getSequenceID());
        MessageID stopMessageID =
            new MessageID(stopTMessageID.getTimestamp(), stopTMessageID.getSequenceID());
        com.etao.hadoop.hbase.queue.client.Scan scan =
            new com.etao.hadoop.hbase.queue.client.Scan(startMessageID, stopMessageID);
        if (null != topics) {
          for (ByteBuffer topic : topics) {
            scan.addTopic(getBytes(topic));
          }
        }

        HQueueScanner scanner =
            new HQueueScanner(queue.getQueueScanner(scan), System.currentTimeMillis(),
                queue.getQueueName());
        return addQueueScanner(scanner);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public TMessage queueScannerGet(int id) throws IOError, IllegalArgument, TException {
      List<TMessage> tMessages = queueScannerGetList(id, 1);
      if (tMessages.isEmpty()) {
        return null;
      } else {
        return tMessages.get(0);
      }
    }

    @Override
    public List<TMessage> queueScannerGetList(int id, int nbMessages) throws IOError,
        IllegalArgument, TException {
      HQueueScanner scanner = getQueueScanner(id);
      if (null == scanner) {
        String message = "GET queue scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("GET queue scanner ID is invalid");
      }

      Message[] messages = null;
      try {
        messages = scanner.getInternalScanner().next(nbMessages);
        if (null == messages) {
          scanner.getInternalScanner().close();
          removeQueueScanner(id);
          return new ArrayList<TMessage>();
        }
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(e.getMessage());
      }

      scanner.updateTime();
      return ThriftUtilities.messageFromHBase(messages);
    }

    @Override
    public Map<ByteBuffer, ColumnDescriptor> getColumnDescriptors(
        ByteBuffer tableName) throws IOError, TException {
      
      Table table = null;
      try {
        TreeMap<ByteBuffer, ColumnDescriptor> columns =
          new TreeMap<ByteBuffer, ColumnDescriptor>();

        table = getTable(tableName);
        HTableDescriptor desc = table.getTableDescriptor();

        for (HColumnDescriptor e : desc.getFamilies()) {
          ColumnDescriptor col = ThriftUtilities.colDescFromHbase(e);
          columns.put(col.name, col);
        }
        return columns;
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally {
        closeTable(table);
      }
    }

    @Deprecated
    @Override
    public List<TCell> getRowOrBefore(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer family) throws IOError {
      try {
        Result result = getRowOrBefore(getBytes(tableName), getBytes(row), getBytes(family));
        return ThriftUtilities.cellFromHBase(result.rawCells());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    @Override
    public TRegionInfo getRegionInfo(ByteBuffer searchRow) throws IOError {
      try {
        byte[] row = getBytes(searchRow);
        Result startRowResult =
            getRowOrBefore(TableName.META_TABLE_NAME.getName(), row, HConstants.CATALOG_FAMILY);

        if (startRowResult == null) {
          throw new IOException("Cannot find row in "+ TableName.META_TABLE_NAME+", row="
                                + Bytes.toStringBinary(row));
        }

        // find region start and end keys
        HRegionInfo regionInfo = HRegionInfo.getHRegionInfo(startRowResult);
        if (regionInfo == null) {
          throw new IOException("HRegionInfo REGIONINFO was null or " +
                                " empty in Meta for row="
                                + Bytes.toStringBinary(row));
        }
        TRegionInfo region = new TRegionInfo();
        region.setStartKey(regionInfo.getStartKey());
        region.setEndKey(regionInfo.getEndKey());
        region.id = regionInfo.getRegionId();
        region.setName(regionInfo.getRegionName());
        region.version = regionInfo.getVersion();

        // find region assignment to server
        ServerName serverName = HRegionInfo.getServerName(startRowResult);
        if (serverName != null) {
          region.setServerName(Bytes.toBytes(serverName.getHostname()));
          region.port = serverName.getPort();
        }
        return region;
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }

    private void closeTable(Table table) throws IOError
    {
      try{
        if(table != null){
          table.close();
        }
      } catch (IOException e){
        LOG.error(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      }
    }
    
    private Result getRowOrBefore(byte[] tableName, byte[] row, byte[] family) throws IOException {
      Scan scan = new Scan(row);
      scan.setReversed(true);
      scan.addFamily(family);
      scan.setStartRow(row);
      Table table = getTable(tableName);      
      try (ResultScanner scanner = table.getScanner(scan)) {
        return scanner.next();
      } finally{
        if(table != null){
          table.close();
        }
      }
    }

    private void initMetrics(ThriftMetrics metrics) {
      this.metrics = metrics;
    }

    @Override
    public void increment(TIncrement tincrement) throws IOError, TException {

      if (tincrement.getRow().length == 0 || tincrement.getTable().length == 0) {
        throw new TException("Must supply a table and a row key; can't increment");
      }

      if (conf.getBoolean(COALESCE_INC_KEY, false)) {
        this.coalescer.queueIncrement(tincrement);
        return;
      }

      Table table = null;
      try {
        table = getTable(tincrement.getTable());
        Increment inc = ThriftUtilities.incrementFromThrift(tincrement);
        table.increment(inc);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
        closeTable(table);
      }
    }

    @Override
    public void incrementRows(List<TIncrement> tincrements) throws IOError, TException {
      if (conf.getBoolean(COALESCE_INC_KEY, false)) {
        this.coalescer.queueIncrements(tincrements);
        return;
      }
      for (TIncrement tinc : tincrements) {
        increment(tinc);
      }
    }

    @Override
    public List<TCell> append(TAppend tappend) throws IOError, TException {
      if (tappend.getRow().length == 0 || tappend.getTable().length == 0) {
        throw new TException("Must supply a table and a row key; can't append");
      }

      Table table = null;
      try {
        table = getTable(tappend.getTable());
        Append append = ThriftUtilities.appendFromThrift(tappend);
        Result result = table.append(append);
        return ThriftUtilities.cellFromHBase(result.rawCells());
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } finally{
          closeTable(table);
      }
    }

    @Override
    public boolean checkAndPut(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        ByteBuffer value, Mutation mput, Map<ByteBuffer, ByteBuffer> attributes) throws IOError,
        IllegalArgument, TException {
      Put put;
      try {
        put = new Put(getBytes(row), HConstants.LATEST_TIMESTAMP);
        addAttributes(put, attributes);

        byte[][] famAndQf = KeyValue.parseColumn(getBytes(mput.column));

        put.addImmutable(famAndQf[0], famAndQf[1], mput.value != null ? getBytes(mput.value)
            : HConstants.EMPTY_BYTE_ARRAY);

        put.setDurability(mput.writeToWAL ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage(), e);
        throw new IllegalArgument(Throwables.getStackTraceAsString(e));
      }

      Table table = null;
      try {
        table = getTable(tableName);
        byte[][] famAndQf = KeyValue.parseColumn(getBytes(column));
        return table.checkAndPut(getBytes(row), famAndQf[0], famAndQf[1],
          value != null ? getBytes(value) : HConstants.EMPTY_BYTE_ARRAY, put);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
        throw new IOError(Throwables.getStackTraceAsString(e));
      } catch (IllegalArgumentException e) {
        LOG.warn(e.getMessage(), e);
        throw new IllegalArgument(Throwables.getStackTraceAsString(e));
      } finally {
          closeTable(table);
      }
    }

    @Override
    public void putMessageWithPid(ByteBuffer queueName, short partitionID, TMessage tMessage)
        throws IOError, IllegalArgument, TException {
      try {
        HQueue queue = getQueue(queueName);
        byte[] topic = tMessage.getTopic();
        byte[] value = tMessage.getValue();
        Message message = new Message(partitionID, topic, value);
        queue.put(message);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void putMessage(ByteBuffer queueName, TMessage tMessage) throws IOError,
        IllegalArgument, TException {
      try {
        HQueue queue = getQueue(queueName);
        byte[] topic = tMessage.getTopic();
        byte[] value = tMessage.getValue();
        Message message = new Message(topic, value);
        queue.put(message);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void
    putMessagesWithPid(ByteBuffer queueName, short partitionID, List<TMessage> tMessages)
        throws IOError, IllegalArgument, TException {
      try {
        HQueue queue = getQueue(queueName);
        List<Message> messages = new ArrayList<Message>(tMessages.size());
        for (TMessage tMessage : tMessages) {
          byte[] topic = tMessage.getTopic();
          byte[] value = tMessage.getValue();
          Message message = new Message(partitionID, topic, value);
          messages.add(message);
        }
        queue.put(messages);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void putMessages(ByteBuffer queueName, List<TMessage> tMessages) throws IOError,
        IllegalArgument, TException {
      try {
        HQueue queue = getQueue(queueName);
        List<Message> messages = new ArrayList<Message>(tMessages.size());
        for (TMessage tMessage : tMessages) {
          byte[] topic = tMessage.getTopic();
          byte[] value = tMessage.getValue();
          Message message = new Message(topic, value);
          messages.add(message);
        }
        queue.put(messages);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void messageScannerClose(int id) throws IOError, IllegalArgument, TException {
      HQueueMessageScanner scanner = removeMessageScanner(id);
      if (scanner == null) {
        String message = "CLOSE message scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("CLOSE message scanner ID is invalid");
      }

      scanner.getInternalScanner().close();
    }

    @Override
    public void partitionScannerClose(int id) throws IOError, IllegalArgument, TException {
      HQueuePartitionScanner scanner = removePartitionScanner(id);
      if (scanner == null) {
        String message = "CLOSE partition scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("CLOSE partition scanner ID is invalid");
      }

      scanner.getInternalScanner().close();
    }

    @Override
    public void queueScannerClose(int id) throws IOError, IllegalArgument, TException {
      HQueueScanner scanner = removeQueueScanner(id);
      if (scanner == null) {
        String message = "CLOSE queue scanner ID is invalid";
        LOG.warn(message);
        throw new IllegalArgument("CLOSE queue scanner ID is invalid");
      }

      scanner.getInternalScanner().close();
    }

    @Override
    public List<ByteBuffer> getQueueLocations(ByteBuffer queueName) throws IOError,
        IllegalArgument, TException {
      List<ByteBuffer> locations = new ArrayList<ByteBuffer>();
      try {
        LOG.info("queue name is " + Bytes.toString(getBytes(queueName)));
        HQueue queue = getQueue(getBytes(queueName));
        LOG.info("beging get partitions " + Thread.currentThread().getName());
        Map<Short, String> partitions = queue.getPartitions();
        locations = new ArrayList<ByteBuffer>(partitions.size());
        for (String location : partitions.values()) {
          locations.add(ByteBuffer.wrap(Bytes.toBytes(location)));
        }

        LOG.info("end get partitions " + Thread.currentThread().getName());
      } catch (IOException e) {
        LOG.warn("", e);
      }

      return locations;
    }

    /**
     * Class that's used to clean the scanners that aren't closed after the defined period of time.
     * Only works for the new scanners
     */
    class ScannerCleanerChore extends ScheduledChore {
      final int timeout;

      public ScannerCleanerChore(String name, final int timeout, final int interval,
          final Stoppable stopper) {
        super(name, stopper, interval);
        this.timeout = timeout;
      }

      @Override
      protected void chore() {
        // Iterate over all the current scanners
        for (Map.Entry<Integer, HTableScanner> entry : scannerMap.entrySet()) {
          HTableScanner scanner = entry.getValue();
          // if over the timeout, clean
          long timeSpend = System.currentTimeMillis() - scanner.getLastTimeUsed();
          if (timeSpend > this.timeout) {
            Integer key = entry.getKey();
            LOG.info("We are closing a scanner that has been running for a long time (" + timeSpend
                + " ms) on the table [" + scanner.getTableName() + "]");
            scannerMap.remove(key);
            // This is normally expired
            try {
              scanner.getInternalScanner().close();
            } catch (Exception e) {
              if (e instanceof UnknownScannerException) {
                // already closed on Region Server
              } else {
                LOG.warn("", e);
              }
            }
          }
        }

        if (null != metrics) {
          metrics.setTableScannerNum(scannerMap.size());
        }

        for (Map.Entry<Integer, HQueueMessageScanner> entry : messageScannerMap.entrySet()) {
          HQueueMessageScanner scanner = entry.getValue();
          // if over the timeout, clean
          long timeSpend = System.currentTimeMillis() - scanner.getLastTimeUsed();
          if (timeSpend > this.timeout) {
            Integer key = entry.getKey();
            LOG.info("We are closing a scanner that has been running for a long time (" + timeSpend
                + " ms) on the queue [" + scanner.getQueueName() + "]");
            messageScannerMap.remove(key);
            // This is normally expired
            try {
              scanner.getInternalScanner().close();
            } catch (Exception e) {
              if (e instanceof UnknownScannerException) {
                // already closed on Region Server
              } else {
                LOG.warn("", e);
              }
            }
          }
        }

        if (null != metrics) {
          metrics.setMessageScannerNum(messageScannerMap.size());
        }

        for (Map.Entry<Integer, HQueuePartitionScanner> entry : partitionScannerMap.entrySet()) {
          HQueuePartitionScanner scanner = entry.getValue();
          // if over the timeout, clean
          long timeSpend = System.currentTimeMillis() - scanner.getLastTimeUsed();
          if (timeSpend > this.timeout) {
            Integer key = entry.getKey();
            LOG.info("We are closing a scanner that has been running for a long time (" + timeSpend
                + " ms) on the queue [" + scanner.getQueueName() + "]");
            partitionScannerMap.remove(key);
            // This is normally expired
            try {
              scanner.getInternalScanner().close();
            } catch (Exception e) {
              if (e instanceof UnknownScannerException) {
                // already closed on Region Server
              } else {
                LOG.warn("", e);
              }
            }
          }
        }

        if (null != metrics) {
          metrics.setPartitionScannerNum(partitionScannerMap.size());
        }

        for (Map.Entry<Integer, HQueueScanner> entry : queueScannerMap.entrySet()) {
          HQueueScanner scanner = entry.getValue();
          // if over the timeout, clean
          long timeSpend = System.currentTimeMillis() - scanner.getLastTimeUsed();
          if (timeSpend > this.timeout) {
            Integer key = entry.getKey();
            LOG.info("We are closing a scanner that has been running for a long time (" + timeSpend
                + " ms) on the queue [" + scanner.getQueueName() + "]");
            queueScannerMap.remove(key);
            // This is normally expired
            try {
              scanner.getInternalScanner().close();
            } catch (Exception e) {
              if (e instanceof UnknownScannerException) {
                // already closed on Region Server
              } else {
                LOG.warn("", e);
              }
            }
          }
        }

        if (null != metrics) {
          metrics.setQueueScannerNum(queueScannerMap.size());
        }
      }
    }

    /**
     * This class is just a wrapper around ResultScanner to carry more info about it. It is meant to
     * be recreated with the same ResultScanner over and over as the scanner progresses. We are also
     * including the table name to give more information about the scanners that timeout.
     */
    class HTableScanner {
      private final ResultScanner scanner;
      private final boolean sortColumns;
      private final String tableName;
      private long lastTimeUsed;

      HTableScanner(ResultScanner scanner, long lastTimeUsed, String tableName,
          boolean sortResultColumns) {
        this.lastTimeUsed = lastTimeUsed;
        this.scanner = scanner;
        this.tableName = tableName;
        this.sortColumns = sortResultColumns;
      }

      public void updateTime() {
        this.lastTimeUsed = System.currentTimeMillis();
      }

      public ResultScanner getInternalScanner() {
        return scanner;
      }

      public boolean isColumnSorted() {
        return sortColumns;
      }

      public String getTableName() {
        return tableName;
      }

      public long getLastTimeUsed() {
        return lastTimeUsed;
      }
    }

    /**
     * This class is just a wrapper around MessageScanner to carry more info about it. It is meant
     * to be recreated with the same MessageScanner over and over as the scanner progresses. We are
     * also including the queue name to give more information about the scanners that timeout.
     */
    class HQueueMessageScanner {
      private final MessageScanner scanner;
      private final String queueName;
      private long lastTimeUsed;

      HQueueMessageScanner(MessageScanner scanner, long lastTimeUsed, String queueName) {
        this.lastTimeUsed = lastTimeUsed;
        this.scanner = scanner;
        this.queueName = queueName;
      }

      public void updateTime() {
        this.lastTimeUsed = System.currentTimeMillis();
      }

      public MessageScanner getInternalScanner() {
        return scanner;
      }

      public String getQueueName() {
        return queueName;
      }

      public long getLastTimeUsed() {
        return lastTimeUsed;
      }
    }

    /**
     * This class is just a wrapper around PartitionScanner to carry more info about it. It is meant
     * to be recreated with the same PartitionScanner over and over as the scanner progresses. We
     * are also including the queue name to give more information about the scanners that timeout.
     */
    class HQueuePartitionScanner {
      private final PartitionScanner scanner;
      private final String queueName;
      private long lastTimeUsed;

      HQueuePartitionScanner(PartitionScanner scanner, long lastTimeUsed, String queueName) {
        this.lastTimeUsed = lastTimeUsed;
        this.scanner = scanner;
        this.queueName = queueName;
      }

      public void updateTime() {
        this.lastTimeUsed = System.currentTimeMillis();
      }

      public PartitionScanner getInternalScanner() {
        return scanner;
      }

      public String getQueueName() {
        return queueName;
      }

      public long getLastTimeUsed() {
        return lastTimeUsed;
      }
    }

    /**
     * This class is just a wrapper around QueueScanner to carry more info about it. It is meant to
     * be recreated with the same QueueScanner over and over as the scanner progresses. We are also
     * including the queue name to give more information about the scanners that timeout.
     */
    class HQueueScanner {
      private final QueueScanner scanner;
      private final String queueName;
      private long lastTimeUsed;

      HQueueScanner(QueueScanner scanner, long lastTimeUsed, String queueName) {
        this.lastTimeUsed = lastTimeUsed;
        this.scanner = scanner;
        this.queueName = queueName;
      }

      public void updateTime() {
        this.lastTimeUsed = System.currentTimeMillis();
      }

      public QueueScanner getInternalScanner() {
        return scanner;
      }

      public String getQueueName() {
        return queueName;
      }

      public long getLastTimeUsed() {
        return lastTimeUsed;
      }
    }
  }

  static Stoppable STOPPABLE = new Stoppable() {
    final AtomicBoolean stop = new AtomicBoolean(false);

    @Override
    public boolean isStopped() {
      return this.stop.get();
    }

    @Override
    public void stop(String why) {
      LOG.info("STOPPING BECAUSE: " + why);
      this.stop.set(true);
    }
  };

  /**
   * Adds all the attributes into the Operation object
   */
  private static void addAttributes(OperationWithAttributes op,
    Map<ByteBuffer, ByteBuffer> attributes) {
    if (attributes == null || attributes.size() == 0) {
      return;
    }
    for (Map.Entry<ByteBuffer, ByteBuffer> entry : attributes.entrySet()) {
      String name = Bytes.toStringBinary(getBytes(entry.getKey()));
      byte[] value =  getBytes(entry.getValue());
      op.setAttribute(name, value);
    }
  }

  public static void registerFilters(Configuration conf) {
    String[] filters = conf.getStrings("hbase.thrift.filters");
    if(filters != null) {
      for(String filterClass: filters) {
        String[] filterPart = filterClass.split(":");
        if(filterPart.length != 2) {
          LOG.warn("Invalid filter specification " + filterClass + " - skipping");
        } else {
          ParseFilter.registerFilter(filterPart[0], filterPart[1]);
        }
      }
    }
  }
}
