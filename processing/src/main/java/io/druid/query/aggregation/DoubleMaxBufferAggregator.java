/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public abstract class DoubleMaxBufferAggregator extends BufferAggregator.Abstract
{
  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, Double.NEGATIVE_INFINITY);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  public static DoubleMaxBufferAggregator create(final FloatColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new DoubleMaxBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          final Float v2 = selector.get();
          if (v2 != null) {
            final double v1 = buf.getDouble(position);
            if (Double.compare(v1, (double) v2) < 0) {
              buf.putDouble(position, v2);
            }
          }
        }
      };
    } else {
      return new DoubleMaxBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          if (predicate.matches()) {
            final Float v2 = selector.get();
            if (v2 != null) {
              final double v1 = buf.getDouble(position);
              if (Double.compare(v1, (double) v2) < 0) {
                buf.putDouble(position, v2);
              }
            }
          }
        }
      };
    }
  }

  public static DoubleMaxBufferAggregator create(final DoubleColumnSelector selector, final ValueMatcher predicate)
  {
    if (predicate == null || predicate == ValueMatcher.TRUE) {
      return new DoubleMaxBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          final Double v2 = selector.get();
          if (v2 != null) {
            final double v1 = buf.getDouble(position);
            if (Double.compare(v1, v2) < 0) {
              buf.putDouble(position, v2);
            }
          }
        }
      };
    } else {
      return new DoubleMaxBufferAggregator()
      {
        @Override
        public final void aggregate(ByteBuffer buf, int position)
        {
          if (predicate.matches()) {
            final Double v2 = selector.get();
            if (v2 != null) {
              final double v1 = buf.getDouble(position);
              if (Double.compare(v1, v2) < 0) {
                buf.putDouble(position, v2);
              }
            }
          }
        }
      };
    }
  }
}
