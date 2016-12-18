/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.filter;

import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Simple filter that returns first N columns on row only. This filter was written to test filters
 * in Get and as soon as it gets its quota of columns, {@link #filterAllRemaining()} returns true.
 * This makes this filter unsuitable as a Scan filter.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ColumnPrefixCountGetFilter extends FilterBase {
  protected byte[] prefix = null;
  private int limit = 0;
  private int count = 0;

  public ColumnPrefixCountGetFilter(final byte[] prefix, final int n) {
    Preconditions.checkArgument(n >= 0, "limit be positive %s", n);
    this.prefix = prefix;
    this.limit = n;
  }

  public int getLimit() {
    return limit;
  }

  public byte[] getPrefix() {
    return prefix;
  }

  @Override
  public boolean filterAllRemaining() {
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    if (null == this.prefix || null == v.getQualifierArray()) {
      return ReturnCode.INCLUDE;
    }

    int qualifierLength = v.getQualifierLength();
    if (this.prefix.length >= qualifierLength) {
      return ReturnCode.INCLUDE;
    }

    byte[] qualifierArray = v.getQualifierArray();
    int qualifierOffset = v.getQualifierOffset();

    int cmp =
        Bytes.compareTo(qualifierArray, qualifierOffset, this.prefix.length, this.prefix, 0,
          this.prefix.length);
    if (0 == cmp) {
      this.count++;
      return (this.count > this.limit ? ReturnCode.SKIP : ReturnCode.INCLUDE);
    }

    return ReturnCode.INCLUDE;
  }

  @Override
  public void reset() {
    this.count = 0;
  }

  public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 2, "Expected 2 but got: %s",
      filterArguments.size());
    byte[] prefix = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    int limit = ParseFilter.convertByteArrayToInt(filterArguments.get(1));
    return new ColumnPrefixCountGetFilter(prefix, limit);
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {
    FilterProtos.ColumnPrefixCountGetFilter.Builder builder =
        FilterProtos.ColumnPrefixCountGetFilter.newBuilder();
    if (null != this.prefix) builder.setPrefix(ByteStringer.wrap(this.prefix));
    builder.setLimit(this.limit);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link ColumnPrefixCountGetFilter} instance
   * @return An instance of {@link ColumnPrefixCountGetFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static ColumnPrefixCountGetFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    FilterProtos.ColumnPrefixCountGetFilter proto;
    try {
      proto = FilterProtos.ColumnPrefixCountGetFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new ColumnPrefixCountGetFilter(proto.getPrefix().toByteArray(), proto.getLimit());
  }

  /**
   * @param o
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof ColumnPrefixCountGetFilter)) return false;

    ColumnPrefixCountGetFilter other = (ColumnPrefixCountGetFilter) o;
    return Bytes.equals(this.getPrefix(), other.getPrefix()) && this.getLimit() == other.getLimit();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " " + Bytes.toString(this.prefix) + " " + this.limit;
  }
}
