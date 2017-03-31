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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.segment.ColumnSelectorFactory;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.*;

public class TimestampMaxAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 31;

  private final String name;
  private final String fieldName;
  private final String timeFormat;

  private TimestampSpec timestampSpec;

  @Inject
  private static Properties properties;

  @JsonCreator
  public TimestampMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("timeFormat") String timeFormat
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.timeFormat = timeFormat;

    this.timestampSpec = new TimestampSpec(fieldName, timeFormat, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new TimestampMaxAggregator(name, metricFactory.makeObjectColumnSelector(fieldName), timestampSpec);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new TimestampMaxBufferAggregator(metricFactory.makeObjectColumnSelector(fieldName), timestampSpec);
  }

  @Override
  public Comparator getComparator()
  {
    return TimestampMaxAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return TimestampMaxAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new TimestampMaxAggregatorFactory(name, name, timeFormat);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new TimestampMaxAggregatorFactory(fieldName, fieldName, timeFormat));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return new DateTime((long)object);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public String getTimeFormat()
  {
    return timeFormat;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);

    return ByteBuffer.allocate(1 + fieldNameBytes.length)
        .put(CACHE_TYPE_ID).put(fieldNameBytes).array();
  }

  @Override
  public String getTypeName()
  {
    return "long";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return Long.MIN_VALUE;
  }

  @Override
  public String toString()
  {
    return "TimestampMaxAggregatorFactory{" +
        "fieldName='" + fieldName + '\'' +
        ", name='" + name + '\'' +
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

    TimestampMaxAggregatorFactory that = (TimestampMaxAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = fieldName != null ? fieldName.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}