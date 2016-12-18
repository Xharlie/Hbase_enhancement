/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.client;

import com.google.protobuf.BlockingRpcChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class TestClientRpcClientFail {

  final Log LOG = LogFactory.getLog(getClass());
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] VALUE = Bytes.toBytes("testValue");
  protected static int SLAVES = 1;

  /**
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
    // Set the custom RPC client with do random falures as the client
    TEST_UTIL.getConfiguration().set(
        RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
        RandomFailRpcClient.class.getName());
  }

  /**
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test that a client that random fails an RPC to the master retries properly and doesn't throw
   * any unexpected exceptions.
   */
  @Test
  public void testAdminRandomRpcFail() throws Exception {
    int initialInvocations = RandomFailRpcClient.invocations.get();
    RandomFailRpcClient rpcClient = (RandomFailRpcClient) RpcClientFactory
        .createClient(TEST_UTIL.getConfiguration(), TEST_UTIL.getClusterKey());

    try {
      for (int i = 0; i < 10; ++i) {
        // Ensure the HBaseAdmin uses a new connection by changing Configuration.
        Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
        conf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf(-1));
        Admin admin = null;
        Connection connection = null;
        try {
          connection = ConnectionFactory.createConnection(conf);
          admin = connection.getAdmin();
          // run some admin commands
          HBaseAdmin.checkHBaseAvailable(conf);
          admin.setBalancerRunning(false, false);
        } catch (MasterNotRunningException e) {
          // Since we are randomly throwing Exceptions, it is possible to get
          // a MasterNotRunningException.  It's a bug if we get other exceptions.
        } finally {
          if (admin != null) {
            admin.close();
            if (admin.getConnection().isClosed()) {
              rpcClient = (RandomFailRpcClient) RpcClientFactory
                  .createClient(TEST_UTIL.getConfiguration(), TEST_UTIL.getClusterKey());
            }
          }
          if (connection != null) {
            connection.close();
          }
        }
      }
      // Ensure the RandomFailRpcEngine is actually being used.
      assertTrue(RandomFailRpcClient.invocations.get() > initialInvocations);
    } finally {
      rpcClient.close();
    }
  }

  /**
   * Test that a client random fails an RPC to the RS retries properly and doesn't throw any
   * unexpected exceptions.
   */
  @Test
  public void testRegionServerRandomRpcFail() throws Exception {
    final TableName testTableName = TableName.valueOf("testRegionServerRandomRpcFail");
    TEST_UTIL.createTable(testTableName, FAMILY);

    int initialInvocations = RandomFailRpcClient.invocations.get();
    RandomFailRpcClient rpcClient = (RandomFailRpcClient) RpcClientFactory
        .createClient(TEST_UTIL.getConfiguration(), TEST_UTIL.getClusterKey());
    try {
      for (int i = 0; i < 10; ++i) {
        // Ensure the HTable uses a new connection by changing Configuration.
        Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
        conf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, String.valueOf(-1));
        Table htable = null;
        Connection connection = null;
        try {
          connection = ConnectionFactory.createConnection(conf);
          htable = connection.getTable(testTableName);
          // run some region server commands
          // always do put first
          if (i == 0 || RANDOM.nextDouble() < 0.5) {
            Put put = new Put(Bytes.toBytes("row"));
            put.add(FAMILY, QUALIFIER, VALUE);
            htable.put(put);
          } else {
            Get get = new Get(Bytes.toBytes("row"));
            get.addColumn(FAMILY, QUALIFIER);
            htable.get(get);
          }
        } finally {
          if (htable != null) {
            htable.close();
          }
          if (connection != null) {
            connection.close();
          }
        }
      }
      // Ensure the RandomFailRpcEngine is actually being used.
      assertTrue(RandomFailRpcClient.invocations.get() > initialInvocations);
    } finally {
      rpcClient.close();
    }
  }

  /**
   * Rpc Channel implementation with RandomTimeoutBlockingRpcChannel
   */
  public static class RandomFailRpcClient extends RpcClientImpl {

    private static final Log LOG = LogFactory.getLog(RandomFailRpcClient.class);
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final double CHANCE_OF_FAIL = 0.5;
    private static AtomicInteger invocations = new AtomicInteger();

    public RandomFailRpcClient(Configuration conf, String clusterId, SocketAddress localAddr) {
      super(conf, clusterId, localAddr);
    }

    // Return my own instance, one that does random failures
    @Override
    public BlockingRpcChannel createBlockingRpcChannel(ServerName sn,
        User ticket, int rpcTimeout)
        throws IOException {
      invocations.getAndIncrement();
      if (RANDOM.nextFloat() < CHANCE_OF_FAIL) {
        ServerName invalidSn =
            ServerName.valueOf("random_invalid_host", sn.getPort(), sn.getStartcode());
        return super.createBlockingRpcChannel(invalidSn, ticket, rpcTimeout);
      } else {
        return super.createBlockingRpcChannel(sn, ticket, rpcTimeout);
      }

    }
  }
}
