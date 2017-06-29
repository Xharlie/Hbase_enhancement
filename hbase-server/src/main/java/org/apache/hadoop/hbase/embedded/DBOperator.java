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
package org.apache.hadoop.hbase.embedded;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class DBOperator implements Table {
  final EmbeddedHBase eHBase;
  final Table table;

  public DBOperator(EmbeddedHBase eHBase, Table table) {
    this.eHBase = eHBase;
    this.table = table;
  }

  @Override
  public TableName getName() {
    return table.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return table.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return table.getTableDescriptor();
  }

  public abstract void snapshot(final String snapshotName) throws IOException;

  public abstract void snapshot(final String snapshotName, HBaseProtos.SnapshotDescription.Type type) throws IOException;

  public abstract void deleteSnapshot(String snapshotName) throws IOException;

  public abstract void restoreSnapshot(final String snapshotName, String tableName)
      throws IOException;
}
