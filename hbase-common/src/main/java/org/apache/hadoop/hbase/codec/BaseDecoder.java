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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.Cell;

/**
 * TODO javadoc
 */
@InterfaceAudience.Private
public abstract class BaseDecoder implements Codec.Decoder {
  protected static final Log LOG = LogFactory.getLog(BaseDecoder.class);
  protected final InputStream in;
  private Cell current = null;
  private final boolean remainAwareIS;

  // Notice: we partially back-ported changes on BaseDecoder from HBASE-14501
  protected static class PBIS extends PushbackInputStream {
    public PBIS(InputStream in, int size) {
      super(in, size);
    }

    public void resetBuf(int size) {
      this.buf = new byte[size];
      this.pos = size;
    }
  }

  public BaseDecoder(final InputStream in) {
    if (in instanceof ByteBufferInputStream) {
      this.in = in;
      remainAwareIS = true;
    } else {
      this.in = new PBIS(in, 1);
      remainAwareIS = false;
    }
  }

  @Override
  public boolean advance() throws IOException {
    if (remainAwareIS) {
      if (in.available() == 0) {
        return false;
      }
    } else {
      int firstByte = in.read();
      if (firstByte == -1) {
        return false;
      } else {
        ((PBIS) in).unread(firstByte);
      }
    }

    try {
      this.current = parseCell();
    } catch (IOException ioEx) {
      if (!remainAwareIS) {
        ((PBIS) in).resetBuf(1); // reset the buffer in case the underlying stream is read from
                                 // upper layers
      }
      rethrowEofException(ioEx);
    }
    return true;
  }

  private void rethrowEofException(IOException ioEx) throws IOException {
    boolean isEof = false;
    try {
      isEof = this.in.available() == 0;
    } catch (Throwable t) {
      LOG.trace("Error getting available for error message - ignoring", t);
    }
    if (!isEof) throw ioEx;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Partial cell read caused by EOF", ioEx);
    }
    EOFException eofEx = new EOFException("Partial cell read");
    eofEx.initCause(ioEx);
    throw eofEx;
  }

  /**
   * @return extract a Cell
   * @throws IOException
   */
  protected abstract Cell parseCell() throws IOException;

  @Override
  public Cell current() {
    return this.current;
  }
}
