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

package io.druid.query.aggregation.hyperloglog;

import com.google.common.base.Predicate;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public class HyperUniquesBufferAggregator implements BufferAggregator
{
  private static final byte[] EMPTY_BYTES = HyperLogLogCollector.makeEmptyVersionedByteArray();
  private final Predicate predicate;
  private final ObjectColumnSelector selector;

  public HyperUniquesBufferAggregator(
      Predicate predicate,
      ObjectColumnSelector selector
  )
  {
    this.predicate = predicate;
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.put(EMPTY_BYTES);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (predicate.apply(null)) {
      HyperLogLogCollector collector = (HyperLogLogCollector) selector.get();

      if (collector == null) {
        return;
      }

      HyperLogLogCollector.makeCollector(
          (ByteBuffer) buf.duplicate().position(position).limit(
              position
              + HyperLogLogCollector.getLatestNumBytesForDenseStorage()
          )
      ).fold(collector);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    final int size = HyperLogLogCollector.getLatestNumBytesForDenseStorage();
    ByteBuffer dataCopyBuffer = ByteBuffer.allocate(size);
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.limit(position + size);
    dataCopyBuffer.put(mutationBuffer);
    dataCopyBuffer.rewind();
    return HyperLogLogCollector.makeCollector(dataCopyBuffer);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HyperUniquesBufferAggregator does not support getFloat()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HyperUniquesBufferAggregator does not support getDouble()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HyperUniquesBufferAggregator does not support getLong()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
