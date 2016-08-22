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

package io.druid.query.aggregation;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.druid.segment.LongColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public abstract class LongSumBufferAggregator implements BufferAggregator
{
  public static LongSumBufferAggregator create(final LongColumnSelector selector, final Predicate predicate)
  {
    if (predicate == null || predicate == Predicates.alwaysTrue()) {
      return new LongSumBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          buf.putLong(position, buf.getLong(position) + selector.get());
        }
      };
    } else {
      return new LongSumBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          if (predicate.apply(null)) {
            buf.putLong(position, buf.getLong(position) + selector.get());
          }
        }
      };
    }
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, 0L);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
