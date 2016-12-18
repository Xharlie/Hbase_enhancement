package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class NotEmptyFamilySingleColumnValueFilter extends SingleColumnValueFilter {

  public NotEmptyFamilySingleColumnValueFilter(final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, final byte[] value) {
    super(family, qualifier, compareOp, new BinaryComparator(value));
  }

  public NotEmptyFamilySingleColumnValueFilter(final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, final ByteArrayComparable comparator) {
    super(family, qualifier, compareOp, comparator);
  }

  protected NotEmptyFamilySingleColumnValueFilter(final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, ByteArrayComparable comparator, final boolean filterIfMissing,
      final boolean latestVersionOnly) {
    super(family, qualifier, compareOp, comparator, filterIfMissing, latestVersionOnly);
  }

  public boolean isFamilyEssential(byte[] name) {
    return Bytes.equals(name, this.columnFamily);
  }

  public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 4 || filterArguments.size() == 6,
        "Expected 4 or 6 but got: %s", filterArguments.size());
    byte[] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    byte[] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
    CompareOp compareOp = ParseFilter.createCompareOp(filterArguments.get(2));
    ByteArrayComparable comparator =
        ParseFilter.createComparator(ParseFilter.removeQuotesFromByteArray(filterArguments.get(3)));

    if (comparator instanceof RegexStringComparator || comparator instanceof SubstringComparator) {
      if (compareOp != CompareOp.EQUAL && compareOp != CompareOp.NOT_EQUAL) {
        throw new IllegalArgumentException("A regexstring comparator and substring comparator "
            + "can only be used with EQUAL and NOT_EQUAL");
      }
    }

    NotEmptyFamilySingleColumnValueFilter filter =
        new NotEmptyFamilySingleColumnValueFilter(family, qualifier, compareOp, comparator);

    if (filterArguments.size() == 6) {
      boolean filterIfMissing = ParseFilter.convertByteArrayToBoolean(filterArguments.get(4));
      boolean latestVersionOnly = ParseFilter.convertByteArrayToBoolean(filterArguments.get(5));
      filter.setFilterIfMissing(filterIfMissing);
      filter.setLatestVersionOnly(latestVersionOnly);
    }
    return filter;
  }

  FilterProtos.SingleColumnValueFilter convert() {
    FilterProtos.SingleColumnValueFilter.Builder builder =
        FilterProtos.SingleColumnValueFilter.newBuilder();
    if (this.columnFamily != null) {
      builder.setColumnFamily(ByteStringer.wrap(this.columnFamily));
    }
    if (this.columnQualifier != null) {
      builder.setColumnQualifier(ByteStringer.wrap(this.columnQualifier));
    }
    HBaseProtos.CompareType compareOp = CompareType.valueOf(this.compareOp.name());
    builder.setCompareOp(compareOp);
    builder.setComparator(ProtobufUtil.toComparator(this.comparator));
    builder.setFilterIfMissing(this.filterIfMissing);
    builder.setLatestVersionOnly(this.latestVersionOnly);

    return builder.build();
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {
    return convert().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link SingleColumnValueFilter} instance
   * @return An instance of {@link SingleColumnValueFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static NotEmptyFamilySingleColumnValueFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    FilterProtos.SingleColumnValueFilter proto;
    try {
      proto = FilterProtos.SingleColumnValueFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }

    final CompareOp compareOp = CompareOp.valueOf(proto.getCompareOp().name());
    final ByteArrayComparable comparator;
    try {
      comparator = ProtobufUtil.toComparator(proto.getComparator());
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }

    return new NotEmptyFamilySingleColumnValueFilter(
        proto.hasColumnFamily() ? proto.getColumnFamily().toByteArray() : null,
        proto.hasColumnQualifier() ? proto.getColumnQualifier().toByteArray() : null, compareOp,
        comparator, proto.getFilterIfMissing(), proto.getLatestVersionOnly());
  }

  @Override public String toString() {
    return String.format("%s (%s, %s, %s, %s)", this.getClass().getSimpleName(),
        Bytes.toStringBinary(this.columnFamily), Bytes.toStringBinary(this.columnQualifier),
        this.compareOp.name(), Bytes.toStringBinary(this.comparator.getValue()));
  }
}