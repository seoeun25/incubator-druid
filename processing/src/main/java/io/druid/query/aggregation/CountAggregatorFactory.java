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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import io.druid.data.ValueDesc;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatchers;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class CountAggregatorFactory extends AggregatorFactory
{
  public static CountAggregatorFactory of(String name)
  {
    return new CountAggregatorFactory(name);
  }

  public static CountAggregatorFactory of(String name, String fieldName)
  {
    return new CountAggregatorFactory(name, null, fieldName);
  }

  private static final byte[] CACHE_KEY = new byte[]{0x0};

  private final String name;
  private final String fieldName;
  private final String predicate;

  @JsonCreator
  public CountAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name == null ? fieldName : name;
    this.predicate = predicate;
    this.fieldName = fieldName;
    Preconditions.checkNotNull(this.name, "Must have a valid, non-null aggregator name");
  }

  public CountAggregatorFactory(String name)
  {
    this(name, null, null);
  }

  public CountAggregatorFactory(String name, String predicate)
  {
    this(name, predicate, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new CountAggregator(toValueMatcher(metricFactory));
  }

  private ValueMatcher toValueMatcher(ColumnSelectorFactory metricFactory)
  {
    ValueMatcher matcher = null;
    if (fieldName != null) {
      matcher = ColumnSelectors.toMatcher(String.format("!isNull(\"%s\")", fieldName), metricFactory);
    }
    if (predicate != null) {
      matcher = ValueMatchers.and(ColumnSelectors.toMatcher(predicate, metricFactory));
    }
    return matcher == null ? ValueMatcher.TRUE : matcher;
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new CountBufferAggregator(toValueMatcher(metricFactory));
  }

  @Override
  public Comparator getComparator()
  {
    return CountAggregator.COMPARATOR;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<Number> combiner()
  {
    return new Combiner<Number>()
    {
      @Override
      public Number combine(Number param1, Number param2)
      {
        return param1.longValue() + param2.longValue();
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongSumAggregatorFactory(name, name);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of();
  }

  @Override
  public byte[] getCacheKey()
  {
    return CACHE_KEY;
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.LONG;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES;
  }

  @Override
  public String toString()
  {
    return "CountAggregatorFactory{" +
           "name='" + name + '\'' +
           (fieldName == null ? "": ", fieldName='" + fieldName + '\'') +
           (predicate == null ? "": ", predicate='" + predicate + '\'') +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CountAggregatorFactory that = (CountAggregatorFactory) o;

    if (!(Objects.equals(name, that.name))) {
      return false;
    }
    if (!(Objects.equals(fieldName, that.fieldName))) {
      return false;
    }
    if (!(Objects.equals(predicate, that.predicate))) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, predicate);
  }
}
