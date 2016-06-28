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
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public class FloatSumBufferAggregator implements BufferAggregator
{
  private final FloatColumnSelector selector;
  private final Predicate predicate;

  public FloatSumBufferAggregator(FloatColumnSelector selector, Predicate predicate)
  {
    this.selector = selector;
    this.predicate = predicate;
  }

  public FloatSumBufferAggregator(FloatColumnSelector selector)
  {
    this(selector, Predicates.alwaysTrue());
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, 0.0d);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (predicate.apply(null)) {
      buf.putDouble(position, buf.getDouble(position) + selector.get());
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}