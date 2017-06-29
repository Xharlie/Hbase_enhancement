/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.ByteBufferCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.SettableSequenceId;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder.EncodedSeeker;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.hadoop.io.WritableUtils;

@InterfaceAudience.Private
public class RowIndexSeekerV2 implements EncodedSeeker {

  private HFileBlockDecodingContext decodingCtx;
  private final CellComparator comparator;

  // A temp pair object which will be reused by ByteBuff#asSubByteBuffer calls.
  // This avoids too
  // many object creations.
  protected final ObjectIntPair<ByteBuffer> tmpPair = new ObjectIntPair<ByteBuffer>();

  private ByteBuff currentBuffer;
  private SeekerState current = new SeekerState(); // always valid
  private SeekerState previous = new SeekerState(); // may not be valid
  private CustomCell currentCell = new CustomCell();

  private int onDiskSize;
  private int rowNumber;
  private ByteBuff rowOffsets = null;
  private byte fLen;
  private ByteBuffer family = null;
  private long firstTimestamp;
  private boolean binarySearch = false;

  public RowIndexSeekerV2(CellComparator comparator,
      HFileBlockDecodingContext decodingCtx) {
    this.comparator = comparator;
    this.decodingCtx = decodingCtx;
  }

  @Override
  public void setCurrentBuffer(ByteBuff buffer) {
    onDiskSize = buffer.getInt(buffer.limit() - Bytes.SIZEOF_INT);

    // Data part
    ByteBuff dup = buffer.duplicate();
    dup.position(buffer.position());
    dup.limit(buffer.position() + onDiskSize);
    currentBuffer = dup.slice();
    current.currentBuffer = currentBuffer;
    buffer.skip(onDiskSize);

    // Row offset
    rowNumber = buffer.getInt();
    // equals Bytes.SIZEOF_INT * rowNumber
    int totalRowOffsetsLength = rowNumber << 2;
    ByteBuff rowDup = buffer.duplicate();
    rowDup.position(buffer.position());
    rowDup.limit(buffer.position() + totalRowOffsetsLength);
    rowOffsets = rowDup.slice();
    buffer.skip(totalRowOffsetsLength);

    // read family
    fLen = buffer.get();
    family = getSubByteBuffer(buffer, buffer.position(), fLen, tmpPair);
    buffer.skip(fLen);

    firstTimestamp = buffer.getLongAfterPosition(0);

    decodeFirst();
  }

  @Override
  public Cell getKey() {
    return KeyValueUtil.copyToNewKeyValue(currentCell);
  }

  @Override
  public ByteBuffer getValueShallowCopy() {
    return getSubByteBuffer(currentBuffer, current.valueOffset,
        current.valueLength, tmpPair).slice();
  }

  @Override
  public Cell getCell() {
    return current.toCell();
  }

  @Override
  public void rewind() {
    currentBuffer.rewind();
    decodeFirst();
  }

  @Override
  public boolean next() {
    if (!currentBuffer.hasRemaining()) {
      return false;
    }
    whetherMoveToNextRow();
    decodeNext();
    previous.invalidate();
    return true;
  }

  private int binarySearch(Cell seekCell, boolean seekBefore) {
    int low = 0;
    int high = rowNumber - 1;
    int mid = (low + high) >>> 1;
    int comp = 0;
    while (low <= high) {
      mid = (low + high) >>> 1;
      ByteBuffer row = getRow(mid);
      comp = compareRows(row, seekCell);
      if (comp < 0) {
        low = mid + 1;
      } else if (comp > 0) {
        high = mid - 1;
      } else {
        // key found
        if (seekBefore) {
          return mid - 1;
        } else {
          return mid;
        }
      }
    }
    // key not found.
    if (comp > 0) {
      return mid - 1;
    } else {
      return mid;
    }
  }

  private int compareRows(ByteBuffer row, Cell seekCell) {
    if (seekCell instanceof ByteBufferCell) {
      return ByteBufferUtils.compareTo(row, row.position(), row.remaining(),
          ((ByteBufferCell) seekCell).getRowByteBuffer(),
          ((ByteBufferCell) seekCell).getRowPosition(),
          seekCell.getRowLength());
    } else {
      return ByteBufferUtils.compareTo(row, row.position(), row.remaining(),
          seekCell.getRowArray(), seekCell.getRowOffset(),
          seekCell.getRowLength());
    }
  }

  private ByteBuffer getRow(int index) {
    // index * Bytes.SIZEOF_INT
    int offset = rowOffsets.getInt(index << 2);
    ByteBuff block = currentBuffer.duplicate();
    block.position(offset);
    short rowLen = block.getShort();
    block.asSubByteBuffer(block.position(), rowLen, tmpPair);
    ByteBuffer row = tmpPair.getFirst();
    row.position(tmpPair.getSecond()).limit(tmpPair.getSecond() + rowLen);
    return row;
  }

  @Override
  public int seekToKeyInBlock(Cell seekCell, boolean seekBefore) {
    previous.invalidate();
    if (!binarySearch) {
      int index = binarySearch(seekCell, seekBefore);
      if (index < 0) {
        return HConstants.INDEX_KEY_MAGIC; // using optimized index key
      } else {
        decodeAtRow(index);
        decodeNext();
      }
      binarySearch = true;
    }
    do {
      int comp;
      comp = comparator.compareKeyIgnoresMvcc(seekCell, currentCell);
      if (comp == 0) { // exact match
        if (seekBefore) {
          if (!previous.isValid()) {
            // The caller (seekBefore) has to ensure that we are not at the
            // first key in the block.
            throw new IllegalStateException("Cannot seekBefore if "
                + "positioned at the first key in the block: key="
                + Bytes.toStringBinary(seekCell.getRowArray()));
          }
          moveToPrevious();
          return 1;
        }
        return 0;
      }

      if (comp < 0) { // already too large, check previous
        if (previous.isValid()) {
          moveToPrevious();
        } else {
          return HConstants.INDEX_KEY_MAGIC; // using optimized index key
        }
        return 1;
      }

      // move to next, if more data is available
      if (currentBuffer.hasRemaining()) {
        previous.copyFromNext(current);
        whetherMoveToNextRow();
        decodeNext();
      } else {
        break;
      }
    } while (true);

    // we hit the end of the block, not an exact match
    return 1;
  }

  private void moveToPrevious() {
    if (!previous.isValid()) {
      throw new IllegalStateException(
          "Can move back only once and not in first key in the block.");
    }

    SeekerState tmp = previous;
    previous = current;
    current = tmp;

    // move after last key value
    currentBuffer.position(current.nextKvOffset);
    previous.invalidate();
  }

  @Override
  public int compareKey(CellComparator comparator, Cell key) {
    return comparator.compareKeyIgnoresMvcc(key, currentCell);
  }

  protected void decodeFirst() {
    decodeAtRow(0);
    decodeNext();
    previous.invalidate();
  }

  protected void decodeAtRow(int rowIndex) {
    if (rowIndex > rowNumber - 1) {
      return;
    }
    int rowStartOffset = rowOffsets.getInt(rowIndex << 2);
    int rowEndOffset = 0;
    if (rowIndex < rowNumber - 1) {
      rowEndOffset = rowOffsets.getInt((rowIndex + 1) << 2);
    } else {
      rowEndOffset = onDiskSize;
    }
    current.rowIndex = rowIndex;
    current.rowStartOffset = rowStartOffset;
    current.rowEndOffset = rowEndOffset;
    // reset
    current.rowLength = -1;
    currentBuffer.position(rowStartOffset);
  }

  private void whetherMoveToNextRow() {
    if (currentBuffer.position() >= current.rowEndOffset) {
      decodeAtRow(current.rowIndex + 1);
    }
  }

  protected void decodeNext() {
    if (current.rowLength < 0) {
      // Row
      current.rowLength = currentBuffer.getShort();
      current.rowBuffer = getSubByteBuffer(currentBuffer,
          currentBuffer.position(), current.rowLength, tmpPair);
      currentBuffer.skip(current.rowLength);
    }
    // Qualifier
    current.qualifierLength = ByteBuff.readCompressedInt(currentBuffer);
    current.qualifierBuffer = getSubByteBuffer(currentBuffer,
        currentBuffer.position(), current.qualifierLength, tmpPair);
    currentBuffer.skip(current.qualifierLength);
    // Timestamp Type
    current.timestamp = ByteBuff.readVLong(currentBuffer) + firstTimestamp;
    current.type = currentBuffer.get();

    // value part
    current.valueLength = ByteBuff.readCompressedInt(currentBuffer);
    current.valueOffset = currentBuffer.position();
    currentBuffer.skip(current.valueLength);
    if (includesTags()) {
      decodeTags();
    }
    if (includesMvcc()) {
      current.memstoreTS = ByteBuff.readVLong(currentBuffer);
    } else {
      current.memstoreTS = 0;
    }
    current.nextKvOffset = currentBuffer.position();
  }

  protected boolean includesMvcc() {
    return this.decodingCtx.getHFileContext().isIncludesMvcc();
  }

  protected boolean includesTags() {
    return this.decodingCtx.getHFileContext().isIncludesTags();
  }

  protected void decodeTags() {
    current.tagsLength = currentBuffer.getShortAfterPosition(0);
    currentBuffer.skip(Bytes.SIZEOF_SHORT);
    current.tagsOffset = currentBuffer.position();
    currentBuffer.skip(current.tagsLength);
  }

  private static ByteBuffer getSubByteBuffer(ByteBuff buffer, int offset,
      int length, ObjectIntPair<ByteBuffer> pair) {
    buffer.asSubByteBuffer(offset, length, pair);
    ByteBuffer resultBuffer = pair.getFirst().duplicate();
    resultBuffer.position(pair.getSecond());
    resultBuffer.limit(pair.getSecond() + length);
    return resultBuffer;
  }

  protected class SeekerState {
    /**
     * The size of a (key length, value length) tuple that prefixes each entry
     * in a data block.
     */
    public final static int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

    protected ByteBuff currentBuffer;

    protected int rowIndex = -1;
    protected int rowStartOffset = -1;
    protected int rowEndOffset = -1;
    protected short rowLength = 0;
    protected ByteBuffer rowBuffer;

    protected int qualifierLength = 0;
    protected ByteBuffer qualifierBuffer;

    protected int valueOffset = -1;
    protected int valueLength = 0;
    protected int tagsOffset = -1;
    protected int tagsLength = 0;

    protected long timestamp;
    protected byte type;

    protected long memstoreTS;
    protected int nextKvOffset;

    protected boolean isValid() {
      return valueOffset != -1;
    }

    protected void invalidate() {
      valueOffset = -1;
      currentBuffer = null;
    }

    protected long getSequenceId() {
      return memstoreTS;
    }

    /**
     * Copy the state from the next one into this instance (the previous state
     * placeholder). Used to save the previous state when we are advancing the
     * seeker to the next key/value.
     */
    protected void copyFromNext(SeekerState nextState) {
      currentBuffer = nextState.currentBuffer;

      rowIndex = nextState.rowIndex;
      rowStartOffset = nextState.rowStartOffset;
      rowEndOffset = nextState.rowEndOffset;
      rowBuffer = nextState.rowBuffer;
      rowLength = nextState.rowLength;

      qualifierBuffer = nextState.qualifierBuffer;
      qualifierLength = nextState.qualifierLength;
      valueOffset = nextState.valueOffset;
      valueLength = nextState.valueLength;
      tagsOffset = nextState.tagsOffset;
      tagsLength = nextState.tagsLength;

      timestamp = nextState.timestamp;
      type = nextState.type;

      memstoreTS = nextState.memstoreTS;
      nextKvOffset = nextState.nextKvOffset;
    }

    @Override
    public String toString() {
      return CellUtil.getCellKeyAsString(toCell());
    }

    public Cell toCell() {
      return currentCell.shallowCopy();
    }
  }

  private static byte[] getByteArray(ByteBuffer buffer, int length) {
    if (buffer.hasArray()) {
      return buffer.array();
    } else {
      // Just in case getValueArray is called on offheap BB
      byte[] val = new byte[length];
      ByteBufferUtils.copyFromBufferToArray(val, buffer, buffer.position(), 0,
          length);
      return val;
    }
  }

  private static int getByteArrayOffset(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      return buffer.position() + buffer.arrayOffset();
    } else {
      return 0;
    }
  }

  class CustomCell extends ByteBufferCell {

    CustomCell() {
    }

    @Override
    public byte[] getRowArray() {
      return CellUtil.cloneRow(this);
    }

    @Override
    public int getRowOffset() {
      return 0;
    }

    @Override
    public short getRowLength() {
      return current.rowLength;
    }

    @Override
    public byte[] getFamilyArray() {
      return CellUtil.cloneFamily(this);
    }

    @Override
    public int getFamilyOffset() {
      return 0;
    }

    @Override
    public byte getFamilyLength() {
      return fLen;
    }

    @Override
    public byte[] getQualifierArray() {
      return CellUtil.cloneQualifier(this);
    }

    @Override
    public int getQualifierOffset() {
      return 0;
    }

    @Override
    public int getQualifierLength() {
      return current.qualifierLength;
    }

    @Override
    public long getTimestamp() {
      return current.timestamp;
    }

    @Override
    public byte getTypeByte() {
      return current.type;
    }

    @Override
    public long getSequenceId() {
      return current.memstoreTS;
    }

    @Override
    public byte[] getValueArray() {
      return CellUtil.cloneValue(this);
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return current.valueLength;
    }

    @Override
    public byte[] getTagsArray() {
      return CellUtil.cloneTags(this);
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      return current.tagsLength;
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return current.rowBuffer;
    }

    @Override
    public int getRowPosition() {
      return current.rowBuffer.position();
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return family;
    }

    @Override
    public int getFamilyPosition() {
      return family.position();
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return current.qualifierBuffer;
    }

    @Override
    public int getQualifierPosition() {
      return current.qualifierBuffer.position();
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return getSubByteBuffer(currentBuffer, current.valueOffset,
          current.valueLength, tmpPair);
    }

    @Override
    public int getValuePosition() {
      return getValueByteBuffer().position();
    }

    @Override
    public ByteBuffer getTagsByteBuffer() {
      if (current.tagsOffset < 0) {
        return HConstants.EMPTY_BYTE_BUFFER;
      } else {
        return getSubByteBuffer(currentBuffer, current.tagsOffset,
            current.tagsLength, tmpPair);
      }
    }

    @Override
    public int getTagsPosition() {
      return getTagsByteBuffer().position();
    }

    @Override
    public String toString() {
      return CellUtil.getCellKeyAsString(this);
    }

    public Cell shallowCopy() {
      return new ClonedSeekerState(getRowByteBuffer(), getRowLength(),
          getFamilyByteBuffer(), getFamilyLength(), getQualifierByteBuffer(),
          getQualifierLength(), getTimestamp(), getTypeByte(),
          getValueByteBuffer(), getValueLength(), getSequenceId(),
          getTagsByteBuffer(), getTagsLength());
    }

    @Override
    public byte[] getFamily() {
      return CellUtil.cloneFamily(this);
    }

    @Override
    public byte[] getQualifier() {
      return CellUtil.cloneQualifier(this);
    }

    @Override
    public byte[] getRow() {
      return CellUtil.cloneRow(this);
    }

    @Override
    public byte[] getValue() {
      return CellUtil.cloneValue(this);
    }

  }

  protected static class ClonedSeekerState extends ByteBufferCell implements
      HeapSize, SettableSequenceId {
    private static final long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
        + (5 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG)
        + (3 * Bytes.SIZEOF_INT) + (Bytes.SIZEOF_SHORT)
        + (2 * Bytes.SIZEOF_BYTE) + (3 * ClassSize.BYTE_BUFFER));

    private ByteBuffer rowBuffer;
    private short rowLength;
    private ByteBuffer familyBuffer;
    private byte familyLength;
    private ByteBuffer qualifierBuffer;
    private int qualifierLength;
    private long timestamp;
    private byte typeByte;
    private ByteBuffer valueBuffer;
    private int valueLength;
    private ByteBuffer tagBuffer;
    private int tagsLength;
    private long seqId;

    protected ClonedSeekerState(ByteBuffer rowBuffer, short rowLength,
        ByteBuffer familyBuffer, byte familyLength, ByteBuffer qualifierBuffer,
        int qualifierLength, long timeStamp, byte typeByte,
        ByteBuffer valueBuffer, int valueLength, long seqId,
        ByteBuffer tagBuffer, int tagsLength) {
      this.rowBuffer = rowBuffer;
      this.rowLength = rowLength;
      this.familyBuffer = familyBuffer;
      this.familyLength = familyLength;
      this.qualifierBuffer = qualifierBuffer;
      this.qualifierLength = qualifierLength;
      this.timestamp = timeStamp;
      this.typeByte = typeByte;
      this.valueBuffer = valueBuffer;
      this.valueLength = valueLength;
      this.tagBuffer = tagBuffer;
      this.tagsLength = tagsLength;
      setSequenceId(seqId);
    }

    @Override
    public void setSequenceId(long seqId) {
      this.seqId = seqId;
    }

    @Override
    public long heapSize() {
      return FIXED_OVERHEAD + rowLength + familyLength + qualifierLength
          + valueLength + tagsLength;
    }

    @Override
    public byte[] getRowArray() {
      return getByteArray(rowBuffer, getRowLength());
    }

    @Override
    public int getRowOffset() {
      return getByteArrayOffset(rowBuffer);
    }

    @Override
    public short getRowLength() {
      return rowLength;
    }

    @Override
    public byte[] getFamilyArray() {
      return getByteArray(familyBuffer, getFamilyLength());
    }

    @Override
    public int getFamilyOffset() {
      return getByteArrayOffset(familyBuffer);
    }

    @Override
    public byte getFamilyLength() {
      return familyLength;
    }

    @Override
    public byte[] getQualifierArray() {
      return getByteArray(qualifierBuffer, getQualifierLength());
    }

    @Override
    public int getQualifierOffset() {
      return getByteArrayOffset(qualifierBuffer);
    }

    @Override
    public int getQualifierLength() {
      return qualifierLength;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public byte getTypeByte() {
      return typeByte;
    }

    @Override
    public long getSequenceId() {
      return seqId;
    }

    @Override
    public byte[] getValueArray() {
      return getByteArray(valueBuffer, getValueLength());
    }

    @Override
    public int getValueOffset() {
      return getByteArrayOffset(valueBuffer);
    }

    @Override
    public int getValueLength() {
      return valueLength;
    }

    @Override
    public byte[] getTagsArray() {
      return getByteArray(tagBuffer, getTagsLength());
    }

    @Override
    public int getTagsOffset() {
      return getByteArrayOffset(tagBuffer);
    }

    @Override
    public int getTagsLength() {
      return tagsLength;
    }

    @Override
    public String toString() {
      return CellUtil.getCellKeyAsString(this);
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return rowBuffer;
    }

    @Override
    public int getRowPosition() {
      return rowBuffer.position();
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return familyBuffer;
    }

    @Override
    public int getFamilyPosition() {
      return familyBuffer.position();
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return qualifierBuffer;
    }

    @Override
    public int getQualifierPosition() {
      return qualifierBuffer.position();
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return valueBuffer;
    }

    @Override
    public int getValuePosition() {
      return valueBuffer.position();
    }

    @Override
    public ByteBuffer getTagsByteBuffer() {
      return tagBuffer;
    }

    @Override
    public int getTagsPosition() {
      return tagBuffer.position();
    }

    @Override
    public byte[] getFamily() {
      return CellUtil.cloneFamily(this);
    }

    @Override
    public byte[] getQualifier() {
      return CellUtil.cloneQualifier(this);
    }

    @Override
    public byte[] getRow() {
      return CellUtil.cloneRow(this);
    }

    @Override
    public byte[] getValue() {
      return CellUtil.cloneValue(this);
    }
  }

}
