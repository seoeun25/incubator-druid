/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Streams arrays of objects out in the binary format described by GenericIndexed
 */
public class GenericIndexedWriter<T> implements ColumnPartWriter<T>
{
  private static Logger LOG = new Logger(GenericIndexedWriter.class);

  public static GenericIndexedWriter<String> dimWriter(
      IOPeon ioPeon,
      String filenameBase
  )
  {
    return new GenericIndexedWriter<>(
        ioPeon,
        filenameBase,
        ObjectStrategy.STRING_STRATEGY
    );
  }

  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ObjectStrategy<T> strategy;

  private boolean objectsSorted = true;
  private T prevObject = null;

  private CountingOutputStream headerOut = null;
  private CountingOutputStream valuesOut = null;
  int numWritten = 0;

  public GenericIndexedWriter(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy<T> strategy
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
    this.objectsSorted = !(strategy instanceof ObjectStrategy.NotComparable);
  }

  @Override
  public void open() throws IOException
  {
    headerOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("header")));
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("values")));
  }

  @Override
  @SuppressWarnings("unchecked")
  public void add(T objectToWrite) throws IOException
  {
    if (objectsSorted && prevObject != null && strategy.compare(prevObject, objectToWrite) >= 0) {
      objectsSorted = false;
    }

    byte[] bytesToWrite = strategy.toBytes(objectToWrite);

    ++numWritten;
    valuesOut.write(Ints.toByteArray(bytesToWrite.length));
    valuesOut.write(bytesToWrite);

    headerOut.write(Ints.toByteArray((int) valuesOut.getCount()));

    prevObject = objectToWrite;
  }

  private String makeFilename(String suffix)
  {
    return String.format("%s.%s", filenameBase, suffix);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() throws IOException
  {
    headerOut.close();
    valuesOut.close();

    final long numBytesWritten = headerOut.getCount() + valuesOut.getCount();

    Preconditions.checkState(
        headerOut.getCount() == (numWritten * 4),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 4,
        headerOut.getCount()
    );
    Preconditions.checkState(
        numBytesWritten < Integer.MAX_VALUE, "Wrote[%s] bytes, which is too many.", numBytesWritten
    );

    OutputStream metaOut = ioPeon.makeOutputStream(makeFilename("meta"));

    try {
      metaOut.write(0x1);
      metaOut.write(objectsSorted ? 0x1 : 0x0);
      metaOut.write(Ints.toByteArray((int) numBytesWritten + 4));
      metaOut.write(Ints.toByteArray(numWritten));
    }
    finally {
      metaOut.close();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return Byte.BYTES +           // version
           Byte.BYTES +           // flag
           Ints.BYTES +           // numBytesWritten
           Ints.BYTES +           // numElements
           headerOut.getCount() + // header length
           valuesOut.getCount();  // value length
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    byte flag = 0;
    if (objectsSorted) {
      flag |= GenericIndexed.Feature.SORTED.getMask();
    }
    channel.write(ByteBuffer.wrap(new byte[] {GenericIndexed.version, flag}));

    // size + count + header + values
    int length = Ints.checkedCast(headerOut.getCount() + valuesOut.getCount() + Integer.BYTES);
    channel.write(ByteBuffer.wrap(Ints.toByteArray(length)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numWritten)));
    try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(makeFilename("header")))) {
      ByteStreams.copy(input, channel);
    }
    try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(makeFilename("values")))) {
      ByteStreams.copy(input, channel);
    }
  }
}
