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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.query.QueryCacheHelper;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.OrderingSpec;

import java.nio.ByteBuffer;

/**
 * internal class only for group-by
 */
public class DimensionSpecWithOrdering extends BaseFilteredDimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x6;

  private final Direction direction;
  private final String ordering;

  public DimensionSpecWithOrdering(
      @JsonProperty("delegate") DimensionSpec delegate,
      @JsonProperty("direction") Direction direction,
      @JsonProperty("ordering") String ordering
  )
  {
    super(delegate);
    this.direction = direction == null ? Direction.ASCENDING : direction;
    this.ordering = Preconditions.checkNotNull(ordering, "'ordering' cannot be null");
  }

  @JsonProperty
  public Direction getDirection()
  {
    return direction;
  }

  @JsonProperty
  public String getOrdering()
  {
    return ordering;
  }

  public OrderingSpec asOrderingSpec()
  {
    return new OrderingSpec(direction, ordering);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !super.equals(o) || getClass() != o.getClass()) {
      return false;
    }

    DimensionSpecWithOrdering that = (DimensionSpecWithOrdering) o;

    if (!ordering.equals(that.ordering)) {
      return false;
    }
    if (direction != that.direction) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + direction.hashCode();
    result = 31 * result + ordering.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "DimensionSpecWithOrdering{" +
           "dimensionSpec=" + delegate +
           ", direction=" + direction +
           ", ordering='" + ordering + '\'' +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] delegateCacheKey = delegate.getCacheKey();
    byte[] comparatorNameBytes = QueryCacheHelper.computeCacheBytes(ordering);
    ByteBuffer filterCacheKey = ByteBuffer.allocate(2 + delegateCacheKey.length + comparatorNameBytes.length)
                                          .put(CACHE_TYPE_ID)
                                          .put(delegateCacheKey)
                                          .put((byte) direction.ordinal())
                                          .put(comparatorNameBytes);
    return filterCacheKey.array();
  }
}