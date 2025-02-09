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
package org.apache.hadoop.hbase.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Not thread safe!
 * <p>
 * Please note that the reads will cause position movement on wrapped ByteBuffer.
 */
@InterfaceAudience.Private
public class ByteBufferInputStream extends InputStream {

  private ByteBuffer buf;

  public ByteBufferInputStream(ByteBuffer buf) {
      this.buf = buf;
  }

  /**
   * Reads the next byte of data from this input stream. The value byte is returned as an
   * <code>int</code> in the range <code>0</code> to <code>255</code>. If no byte is available
   * because the end of the stream has been reached, the value <code>-1</code> is returned.
   * @return the next byte of data, or <code>-1</code> if the end of the stream has been reached.
   */
  public int read() {
    if (this.buf.hasRemaining()) {
      return (this.buf.get() & 0xff);
    }
    return -1;
  }

  /**
   * Reads the next 4 bytes as an 'int' value from this input stream. If not enough bytes left in
   * stream, throws EOFException
   *
   * @return Next 4 bytes of data read as int
   * @throws IOException if an IO error occurs
   */
  public int readInt() throws IOException{
    if(this.available()<Bytes.SIZEOF_INT){
      throw new EOFException();
    }
    int i = ByteBufferUtils.toInt(this.buf, this.buf.position());
    this.buf.position(this.buf.position() + Bytes.SIZEOF_INT);
    return i;
  }

  /**
   * Reads up to next <code>len</code> bytes of data from buffer into passed array(starting from
   * given offset).
   * @param b the array into which the data is read.
   * @param off the start offset in the destination array <code>b</code>
   * @param len the maximum number of bytes to read.
   * @return the total number of bytes actually read into the buffer, or <code>-1</code> if not even
   *         1 byte can be read because the end of the stream has been reached.
   */
  public int read(byte b[], int off, int len) {
    int avail = available();
    if (avail <= 0) {
      return -1;
    }

    if (len > avail) {
      len = avail;
    }
    if (len <= 0) {
      return 0;
    }

    this.buf.get(b, off, len);
    return len;
  }

  /**
   * Skips <code>n</code> bytes of input from this input stream. Fewer bytes might be skipped if the
   * end of the input stream is reached. The actual number <code>k</code> of bytes to be skipped is
   * equal to the smaller of <code>n</code> and remaining bytes in the stream.
   * @param n the number of bytes to be skipped.
   * @return the actual number of bytes skipped.
   */
  public long skip(long n) {
    long k = Math.min(n, available());
    if (k < 0) {
      k = 0;
    }
    this.buf.position((int) (this.buf.position() + k));
    return k;
  }

  /**
   * @return  the number of remaining bytes that can be read (or skipped
   *          over) from this input stream.
   */
  public int available() {
    return this.buf.remaining();
  }

  public ByteBuffer getBuffer(){
    return this.buf;
  }
}