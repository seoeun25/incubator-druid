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

package io.druid.query.aggregation.covariance;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.Aggregators;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 */
public abstract class CovarianceAggregator implements Aggregator
{
  protected final String name;

  protected final io.druid.query.aggregation.covariance.CovarianceAggregatorCollector holder = new io.druid.query.aggregation.covariance.CovarianceAggregatorCollector();

  public CovarianceAggregator(String name)
  {
    this.name = name;
  }

  @Override
  public void reset()
  {
    holder.reset();
  }

  @Override
  public Object get()
  {
    return holder;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("CovarianceAggregator does not support getFloat()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("CovarianceAggregator does not support getDouble()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("CovarianceAggregator does not support getLong()");
  }

  public static Aggregator create(
      String name,
      final DoubleColumnSelector selector1,
      final DoubleColumnSelector selector2,
      final Predicate<?> predicate
  )
  {
    if (predicate == null || predicate == Predicates.alwaysTrue()) {
      return new CovarianceAggregator(name)
      {
        @Override
        public void aggregate()
        {
          holder.add(selector1.get(), selector2.get());
        }
      };
    } else {
      return new CovarianceAggregator(name)
      {
        @Override
        public void aggregate()
        {
          if (predicate.apply(null)) {
            holder.add(selector1.get(), selector2.get());
          }
        }
      };
    }
  }

  public static Aggregator create(String name, final ObjectColumnSelector selector, final Predicate<?> predicate)
  {
    if (selector == null) {
      return Aggregators.noopAggregator();
    }
    return new CovarianceAggregator(name)
    {
      @Override
      public void aggregate()
      {
        if (predicate.apply(null)) {
          io.druid.query.aggregation.covariance.CovarianceAggregatorCollector.combineValues(holder, selector.get());
        }
      }
    };
  }
}