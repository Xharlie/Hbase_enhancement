/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.security.User;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@Category(MediumTests.class)
public class TestClientTimeouts {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static int SLAVES = 1;

 /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
    // Set the custom RPC client with random timeouts as the client
    TEST_UTIL.getConfiguration().set(
        RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
        RandomTimeoutRpcClient.class.getName());
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test that a client that fails an RPC to the master retries properly and
   * doesn't throw any unexpected exceptions.
   * @throws Exception
   */
  @Test
  public void testAdminTimeout() throws Exception {
    Connection lastConnection = null;
    boolean lastFailed = false;
    int initialInvocations = RandomTimeoutBlockingRpcChannel.invokations.get();
    RandomTimeoutRpcClient rpcClient = (RandomTimeoutRpcClient) RpcClientFactory
        .createClient(TEST_UTIL.getConfiguration(), TEST_UTIL.getClusterKey());

    try {
      for (int i = 0; i < 5 || (lastFailed && i < 100); ++i) {
        lastFailed = false;
        // Ensure the HBaseAdmin uses a new connection by changing Configuration.
        Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
        conf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf(-1));
        HBaseAdmin admin = null;
        try {
          admin = new HBaseAdmin(conf);
          Connection connection = admin.getConnection();
          assertFalse(connection == lastConnection);
          lastConnection = connection;
          // run some admin commands
          HBaseAdmin.checkHBaseAvailable(conf);
          admin.setBalancerRunning(false, false);
        } catch (MasterNotRunningException ex) {
          // Since we are randomly throwing SocketTimeoutExceptions, it is possible to get
          // a MasterNotRunningException.  It's a bug if we get other exceptions.
          lastFailed = true;
        } finally {
          admin.close();
          if (admin.getConnection().isClosed()) {
            rpcClient = (RandomTimeoutRpcClient) RpcClientFactory
                .createClient(TEST_UTIL.getConfiguration(), TEST_UTIL.getClusterKey());
          }
        }
      }
      // Ensure the RandomTimeoutRpcEngine is actually being used.
      assertFalse(lastFailed);
      assertTrue(RandomTimeoutBlockingRpcChannel.invokations.get() > initialInvocations);
    } finally {
      rpcClient.close();
    }
  }

  /**
   * Rpc Channel implementation with RandomTimeoutBlockingRpcChannel
   */
  public static class RandomTimeoutRpcClient extends RpcClientImpl{
    public RandomTimeoutRpcClient(Configuration conf, String clusterId, SocketAddress localAddr) {
      super(conf, clusterId, localAddr);
    }

    // Return my own instance, one that does random timeouts
    @Override
    public BlockingRpcChannel createBlockingRpcChannel(ServerName sn,
        User ticket, int rpcTimeout)
        throws IOException {
      return new RandomTimeoutBlockingRpcChannel(this, sn, ticket, rpcTimeout);
    }
  }

  /**
   * Blocking rpc channel that goes via hbase rpc.
   */
  static class RandomTimeoutBlockingRpcChannel
      extends AbstractRpcClient.BlockingRpcChannelImplementation {
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    public static final double CHANCE_OF_TIMEOUT = 0.3;
    private static AtomicInteger invokations = new AtomicInteger();

    RandomTimeoutBlockingRpcChannel(final RpcClientImpl rpcClient, final ServerName sn,
        final User ticket, final int rpcTimeout)
        throws IOException {
      super(rpcClient, sn, ticket, rpcTimeout);
    }

    @Override
    public Message callBlockingMethod(MethodDescriptor md,
        RpcController controller, Message param, Message returnType)
        throws ServiceException {
      invokations.getAndIncrement();
      if (RANDOM.nextFloat() < CHANCE_OF_TIMEOUT) {
        // throw a ServiceException, becuase that is the only exception type that
        // {@link ProtobufRpcEngine} throws.  If this RpcEngine is used with a different
        // "actual" type, this may not properly mimic the underlying RpcEngine.
        throw new ServiceException(new SocketTimeoutException("fake timeout"));
      }
      return super.callBlockingMethod(md, controller, param, returnType);
    }
  }
}