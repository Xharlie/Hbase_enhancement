/**
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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.metrics.BaseSource;

public interface MetricsHBaseServerSource extends BaseSource {
  String AUTHORIZATION_SUCCESSES_NAME = "authorizationSuccesses";
  String AUTHORIZATION_SUCCESSES_DESC =
      "Number of authorization successes.";
  String AUTHORIZATION_FAILURES_NAME = "authorizationFailures";
  String AUTHORIZATION_FAILURES_DESC =
      "Number of authorization failures.";
  String AUTHENTICATION_SUCCESSES_NAME = "authenticationSuccesses";
  String AUTHENTICATION_SUCCESSES_DESC =
      "Number of authentication successes.";
  String AUTHENTICATION_FAILURES_NAME = "authenticationFailures";
  String AUTHENTICATION_FAILURES_DESC =
      "Number of authentication failures.";
  String SENT_BYTES_NAME = "sentBytes";
  String SENT_BYTES_DESC = "Number of bytes sent.";
  String RECEIVED_BYTES_NAME = "receivedBytes";
  String RECEIVED_BYTES_DESC = "Number of bytes received.";
  String QUEUE_CALL_TIME_NAME = "queueCallTime";
  String QUEUE_CALL_TIME_DESC = "Queue Call Time.";
  String PROCESS_CALL_TIME_NAME = "processCallTime";
  String PROCESS_CALL_TIME_DESC = "Processing call time.";
  String TOTAL_CALL_TIME_NAME = "totalCallTime";
  String TOTAL_CALL_TIME_DESC = "Total call time, including both queued and processing time.";
  String QUEUE_SIZE_NAME = "queueSize";
  String QUEUE_SIZE_DESC = "Number of bytes in the call queues.";
  String RESPONSE_QUEUE_SIZE_NAME = "responseQueueSize";
  String RESPONSE_QUEUE_SIZE_DESC = "Number of bytes in the response queues.";
  String GENERAL_QUEUE_NAME = "numCallsInGeneralQueue";
  String GENERAL_QUEUE_DESC = "Number of calls in the general call queue.";
  String PRIORITY_QUEUE_NAME = "numCallsInPriorityQueue";
  String REPLICATION_QUEUE_NAME = "numCallsInReplicationQueue";
  String REPLICATION_QUEUE_DESC =
      "Number of calls in the replication call queue.";
  String PRIORITY_QUEUE_DESC = "Number of calls in the priority call queue.";
  String RESPONSE_QUEUE_NAME = "numCallsInResponseQueue";
  String RESPONSE_QUEUE_DESC = "Number of calls in the response queue.";
  String NUM_OPEN_CONNECTIONS_NAME = "numOpenConnections";
  String NUM_OPEN_CONNECTIONS_DESC = "Number of open connections.";
  String NUM_ACTIVE_HANDLER_NAME = "numActiveHandler";
  String NUM_ACTIVE_HANDLER_DESC = "Number of active rpc handlers.";

  String EXCEPTIONS_NAME="exceptions";
  String EXCEPTIONS_DESC="Exceptions caused by requests";
  String EXCEPTIONS_TYPE_DESC="Number of requests that resulted in the specified type of Exception";
  String EXCEPTIONS_OOO_NAME="exceptions.OutOfOrderScannerNextException";
  String EXCEPTIONS_BUSY_NAME="exceptions.RegionTooBusyException";
  String EXCEPTIONS_UNKNOWN_NAME="exceptions.UnknownScannerException";
  String EXCEPTIONS_SANITY_NAME="exceptions.FailedSanityCheckException";
  String EXCEPTIONS_MOVED_NAME="exceptions.RegionMovedException";
  String EXCEPTIONS_NSRE_NAME="exceptions.NotServingRegionException";

  void authorizationSuccess();

  void authorizationFailure();

  void authenticationSuccess();

  void authenticationFailure();

  void exception();

  /**
   * Different types of exceptions
   */
  void outOfOrderException();
  void failedSanityException();
  void movedRegionException();
  void notServingRegionException();
  void unknownScannerException();
  void tooBusyException();

  void sentBytes(long count);

  void receivedBytes(int count);

  void dequeuedCall(int qTime);

  void processedCall(int processingTime);

  void queuedAndProcessedCall(int totalTime);
  }
