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
package org.apache.hadoop.hbase.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.io.IOUtils;

/**
 * Codec that does KeyValue version 1 serialization.
 * 
 * <p>Encodes Cell as serialized in KeyValue with total length prefix.
 * This is how KVs were serialized in Puts, Deletes and Results pre-0.96.  Its what would
 * happen if you called the Writable#write KeyValue implementation.  This encoder will fail
 * if the passed Cell is not an old-school pre-0.96 KeyValue.  Does not copy bytes writing.
 * It just writes them direct to the passed stream.
 *
 * <p>If you wrote two KeyValues to this encoder, it would look like this in the stream:
 * <pre>
 * length-of-KeyValue1 // A java int with the length of KeyValue1 backing array
 * KeyValue1 backing array filled with a KeyValue serialized in its particular format
 * length-of-KeyValue2
 * KeyValue2 backing array
 * </pre>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class KeyValueCodec implements Codec {
  public static class KeyValueEncoder extends BaseEncoder {
    public KeyValueEncoder(final OutputStream out) {
      super(out);
    }

    @Override
    public void write(Cell cell) throws IOException {
      checkFlushed();
      // Do not write tags over RPC
      ByteBufferUtils.putInt(this.out, KeyValueUtil.getSerializedSize(cell, false));
      KeyValueUtil.oswrite(cell, out, false);
    }
  }

  public static class KeyValueDecoder extends BaseDecoder {
    public KeyValueDecoder(final InputStream in) {
      super(in);
    }

    protected Cell parseCell() throws IOException {
      if (in instanceof ByteBufferInputStream) {
        // This stream is backed by a ByteBuffer. We can directly read the Cell length from this
        // Buffer and create Cell instance directly on top of it. No need for reading into temp
        // byte[] and create Cell on top of that. It saves lot of garbage.
        ByteBufferInputStream bis = (ByteBufferInputStream) in;
        int len = bis.readInt();
        ByteBuffer buf = bis.getBuffer();
        assert buf.hasArray();
        Cell c = createCell(buf.array(), buf.arrayOffset() + buf.position(), len);
        bis.skip(len);
        return c;
      }
      int len = StreamUtils.readInt(in);
      byte[] bytes = new byte[len];
      IOUtils.readFully(in, bytes, 0, bytes.length);
      return createCell(bytes, 0, len);
    }

    protected Cell createCell(byte[] buf, int offset, int len) {
      return new NoTagsKeyValue(buf, offset, len);
    }
  }

  public static class ByteBufferedKeyValueDecoder implements Codec.Decoder {

    protected final ByteBuffer buf;
    protected Cell current = null;

    public ByteBufferedKeyValueDecoder(ByteBuffer buf) {
      this.buf = buf;
    }

    @Override
    public boolean advance() throws IOException {
      if (this.buf.remaining() <= 0) return false;
      int len = ByteBufferUtils.toInt(buf);
      assert buf.hasArray();
      this.current = createCell(buf.array(), buf.arrayOffset() + buf.position(), len);
      buf.position(buf.position() + len);
      return true;
    }

    @Override
    public Cell current() {
      return this.current;
    }

    protected Cell createCell(byte[] buf, int offset, int len) {
      return new NoTagsKeyValue(buf, offset, len);
    }
  }

  /**
   * Implementation depends on {@link InputStream#available()}
   */
  @Override
  public Decoder getDecoder(final InputStream is) {
    return new KeyValueDecoder(is);
  }

  @Override
  public Decoder getDecoder(ByteBuffer buf) {
    return new ByteBufferedKeyValueDecoder(buf);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    return new KeyValueEncoder(os);
  }
}
