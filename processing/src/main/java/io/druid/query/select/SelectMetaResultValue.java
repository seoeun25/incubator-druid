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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

/**
 */
public class SelectMetaResultValue
{
  private final Schema schema;
  private final Map<String, Integer> perSegmentCounts;
  private final int totalCount;
  private final long estimatedSize;

  @JsonCreator
  public SelectMetaResultValue(
      @JsonProperty("schema") Schema schema,
      @JsonProperty("perSegmentCounts") Map<String, Integer> perSegmentCounts,
      @JsonProperty("estimatedSize") long estimatedSize
  )
  {
    this.schema = schema;
    this.perSegmentCounts = perSegmentCounts;
    int total = 0;
    for (Integer segmentCount : perSegmentCounts.values()) {
      total += segmentCount;
    }
    this.totalCount = total;
    this.estimatedSize = estimatedSize;
  }

  public SelectMetaResultValue(Schema schema)
  {
    this(schema, ImmutableMap.<String, Integer>of(), -1L);
  }

  @JsonProperty
  public Schema getSchema()
  {
    return schema;
  }

  @JsonProperty
  public Map<String, Integer> getPerSegmentCounts()
  {
    return perSegmentCounts;
  }

  @JsonProperty
  public int getTotalCount()
  {
    return totalCount;
  }

  @JsonProperty
  public long getEstimatedSize()
  {
    return estimatedSize;
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

    SelectMetaResultValue that = (SelectMetaResultValue) o;

    if (!Objects.equals(schema, that.schema)) {
      return false;
    }
    if (!Objects.equals(perSegmentCounts, that.perSegmentCounts)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int hash = Objects.hashCode(schema);
    hash = hash * 31 + Objects.hashCode(perSegmentCounts);
    return hash;
  }

  @Override
  public String toString()
  {
    return "SelectMetaResultValue{" +
           "schema=" + schema +
           ", perSegmentCounts=" + perSegmentCounts +
           ", totalCount=" + totalCount +
           ", estimatedSize=" + estimatedSize +
           '}';
  }
}