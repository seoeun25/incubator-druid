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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.druid.common.utils.StringUtils;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class MapVirtualColumn implements VirtualColumn
{
  private static final String MAP_KEY = "__key";
  private static final String MAP_VALUE = "__value";

  private static final byte VC_TYPE_ID = 0x00;

  private final String outputName;
  private final String keyDimension;
  private final String valueDimension;
  private final String valueMetric;

  @JsonCreator
  public MapVirtualColumn(
      @JsonProperty("keyDimension") String keyDimension,
      @JsonProperty("valueDimension") String valueDimension,
      @JsonProperty("valueMetric") String valueMetric,
      @JsonProperty("outputName") String outputName
  )
  {
    Preconditions.checkArgument(keyDimension != null, "key dimension should not be null");
    Preconditions.checkArgument(
        valueDimension == null ^ valueMetric == null,
        "Must have a valid, non-null valueDimension or valueMetric"
    );
    Preconditions.checkArgument(outputName != null, "output name should not be null");

    this.keyDimension = keyDimension;
    this.valueDimension = valueDimension;
    this.valueMetric = valueMetric;
    this.outputName = outputName;
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    final DimensionSelector keySelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(keyDimension));
    if (valueDimension != null) {
      final DimensionSelector valueSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(valueDimension));

      int index = column.indexOf('.');
      if (index < 0) {
        return new ObjectColumnSelector<Map>()
        {
          @Override
          public Class classOfObject()
          {
            return Map.class;
          }

          @Override
          public Map get()
          {
            final IndexedInts keyIndices = keySelector.getRow();
            final IndexedInts valueIndices = valueSelector.getRow();
            if (keyIndices == null || valueIndices == null) {
              return null;
            }
            final int limit = Math.min(keyIndices.size(), valueIndices.size());
            final Map<String, String> map = Maps.newHashMapWithExpectedSize(limit);
            for (int i = 0; i < limit; i++) {
              map.put(
                  keySelector.lookupName(keyIndices.get(i)),
                  valueSelector.lookupName(valueIndices.get(i))
              );
            }
            return map;
          }
        };
      }

      final int keyId = keySelector.lookupId(column.substring(index + 1));
      if (keyId < 0) {
        return new ObjectColumnSelector()
        {
          @Override
          public Class classOfObject()
          {
            return String.class;
          }

          @Override
          public Object get()
          {
            return null;
          }
        };
      }

      return new ObjectColumnSelector<String>()
      {
        @Override
        public Class classOfObject()
        {
          return String.class;
        }

        @Override
        public String get()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final IndexedInts valueIndices = valueSelector.getRow();
          if (keyIndices == null || valueIndices == null) {
            return null;
          }
          final int limit = Math.min(keyIndices.size(), valueIndices.size());
          for (int i = 0; i < limit; i++) {
            if (keyIndices.get(i) == keyId) {
              return valueSelector.lookupName(valueIndices.get(i));
            }
          }
          return null;
        }
      };
    }

    final ObjectColumnSelector<List> valueSelector = factory.makeObjectColumnSelector(valueMetric);

    int index = column.indexOf('.');
    if (index < 0) {
      return new ObjectColumnSelector<Map>()
      {
        @Override
        public Class classOfObject()
        {
          return Map.class;
        }

        @Override
        public Map get()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final List values = valueSelector.get();
          if (keyIndices == null || values == null) {
            return null;
          }
          final int limit = Math.min(keyIndices.size(), values.size());
          final Map<String, Object> map = Maps.newHashMapWithExpectedSize(limit);
          for (int i = 0; i < limit; i++) {
            map.put(keySelector.lookupName(keyIndices.get(i)), values.get(i));
          }
          return map;
        }
      };
    }

    final int keyId = keySelector.lookupId(column.substring(index + 1));
    if (keyId < 0) {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return String.class;
        }

        @Override
        public Object get()
        {
          return null;
        }
      };
    }

    return new ObjectColumnSelector<Object>()
    {
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public Object get()
      {
        final IndexedInts keyIndices = keySelector.getRow();
        final List values = valueSelector.get();
        if (keyIndices == null || values == null) {
          return null;
        }
        final int limit = Math.min(keyIndices.size(), values.size());
        for (int i = 0; i < limit; i++) {
          if (keyIndices.get(i) == keyId) {
            return values.get(i);
          }
        }
        return null;
      }
    };
  }

  @Override
  public FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (selector.classOfObject() == Map.class) {
      throw new UnsupportedOperationException("asFloatMetric");
    }
    return ColumnSelectors.asFloat(selector);
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (selector.classOfObject() == Map.class) {
      throw new UnsupportedOperationException("asDoubleMetric");
    }
    return ColumnSelectors.asDouble(selector);
  }

  @Override
  public LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (selector.classOfObject() == Map.class) {
      throw new UnsupportedOperationException("asLongMetric");
    }
    return ColumnSelectors.asLong(selector);
  }

  @Override
  public DimensionSelector asDimension(String dimension, ColumnSelectorFactory factory)
  {
    int index = dimension.indexOf('.');
    if (index < 0) {
      throw new IllegalArgumentException(dimension + " cannot be used as dimension");
    }
    String target = dimension.substring(index + 1);
    if (MAP_KEY.equals(target)) {
      return factory.makeDimensionSelector(DefaultDimensionSpec.of(keyDimension));
    }
    if (MAP_VALUE.equals(target)) {
      return factory.makeDimensionSelector(DefaultDimensionSpec.of(valueDimension));
    }
    ObjectColumnSelector selector = asMetric(dimension, factory);
    if (selector.classOfObject() != String.class) {
      throw new UnsupportedOperationException("asDimension");
    }
    return VirtualColumns.toDimensionSelector(selector);
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new MapVirtualColumn(keyDimension, valueDimension, valueMetric, outputName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] key = StringUtils.toUtf8(keyDimension);
    byte[] valueDim = StringUtils.toUtf8WithNullToEmpty(valueDimension);
    byte[] valueMet = StringUtils.toUtf8WithNullToEmpty(valueMetric);
    byte[] output = StringUtils.toUtf8(outputName);

    return ByteBuffer.allocate(4 + key.length + valueDim.length + valueMet.length + output.length)
                     .put(VC_TYPE_ID)
                     .put(key).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valueDim).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valueMet).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(output)
                     .array();
  }

  @JsonProperty
  public String getKeyDimension()
  {
    return keyDimension;
  }

  @JsonProperty
  public String getValueDimension()
  {
    return valueDimension;
  }

  @JsonProperty
  public String getValueMetric()
  {
    return valueMetric;
  }

  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public boolean isIndexed(String dimension)
  {
    return false;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MapVirtualColumn)) {
      return false;
    }

    MapVirtualColumn that = (MapVirtualColumn) o;

    if (!keyDimension.equals(that.keyDimension)) {
      return false;
    }
    if (!Objects.equals(valueDimension, that.valueDimension)) {
      return false;
    }
    if (!Objects.equals(valueMetric, that.valueMetric)) {
      return false;
    }
    if (!outputName.equals(that.outputName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = keyDimension.hashCode();
    result = 31 * result + Objects.hashCode(valueDimension);
    result = 31 * result + Objects.hashCode(valueMetric);
    result = 31 * result + outputName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "MapVirtualColumn{" +
           "keyDimension='" + keyDimension + '\'' +
           ", valueDimension='" + valueDimension + '\'' +
           ", valueMetric='" + valueMetric + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
