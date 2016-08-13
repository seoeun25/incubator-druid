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
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

/**
 */
public abstract class DoubleMaxAggregator implements Aggregator
{
  static final Comparator COMPARATOR = DoubleSumAggregator.COMPARATOR;

  static double combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  final String name;
  final Predicate predicate;

  double max;

  public DoubleMaxAggregator(String name, Predicate predicate)
  {
    this.name = name;
    this.predicate = predicate == null ? Predicates.alwaysTrue() : predicate;
    reset();
  }

  @Override
  public void reset()
  {
    max = Double.NEGATIVE_INFINITY;
  }

  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public float getFloat()
  {
    return (float) max;
  }

  @Override
  public long getLong()
  {
    return (long) max;
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
  public void close()
  {
    // no resources to cleanup
  }

  public static class FloatInput extends DoubleMaxAggregator
  {
    private final FloatColumnSelector selector;

    public FloatInput(String name, FloatColumnSelector selector, Predicate predicate)
    {
      super(name, predicate);
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      if (predicate.apply(null)) {
        max = Math.max(max, selector.get());
      }
    }

    @Override
    public Aggregator clone()
    {
      return new FloatInput(name, selector, predicate);
    }
  }

  public static class DoubleInput extends DoubleMaxAggregator
  {
    private final DoubleColumnSelector selector;

    public DoubleInput(String name, DoubleColumnSelector selector, Predicate predicate)
    {
      super(name, predicate);
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      if (predicate.apply(null)) {
        max = Math.max(max, selector.get());
      }
    }

    @Override
    public Aggregator clone()
    {
      return new DoubleInput(name, selector, predicate);
    }
  }
}
