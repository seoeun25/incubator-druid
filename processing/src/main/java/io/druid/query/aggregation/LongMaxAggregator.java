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

import java.util.Comparator;

/**
 */
public class LongMaxAggregator implements Aggregator
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

  static long combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number) lhs).longValue(), ((Number) rhs).longValue());
  }

  private final LongColumnSelector selector;
  private final Predicate predicate;
  private final String name;

  private long max;

  public LongMaxAggregator(String name, LongColumnSelector selector, Predicate predicate)
  {
    this.name = name;
    this.selector = selector;
    this.predicate = predicate == null ? Predicates.alwaysTrue() : predicate;

    reset();
  }

  @Override
  public void aggregate()
  {
    if (predicate.apply(null)) {
      max = Math.max(max, selector.get());
    }
  }

  @Override
  public void reset()
  {
    max = Long.MIN_VALUE;
  }

  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public float getFloat()
  {
    return max;
  }

  @Override
  public long getLong()
  {
    return max;
  }

  @Override
  public double getDouble()
  {
    return max;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new LongMaxAggregator(name, selector, predicate);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
