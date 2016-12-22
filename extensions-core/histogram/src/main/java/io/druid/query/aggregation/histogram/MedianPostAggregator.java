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

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Comparator;
import java.util.Map;

/**
 */
@JsonTypeName("median")
public class MedianPostAggregator extends ApproximateHistogramPostAggregator
{
  @JsonCreator
  public MedianPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    super(name, fieldName);
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    return ((ApproximateHistogramHolder) values.get(this.getFieldName())).getMedian();
  }

  @Override
  public Comparator getComparator()
  {
    return QuantilePostAggregator.COMPARATOR;
  }

  @Override
  public String toString()
  {
    return "MedianPostAggregator{" +
           ", name='" + getName() + '\'' +
           ", fieldName='" + getFieldName() + '\'' +
           '}';
  }
}