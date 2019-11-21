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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.query.select.Schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
@JsonTypeName("explodeMap")
public class ExplodeMapSpec implements LateralViewSpec
{
  private final List<String> columns;
  private final String keyAlias;
  private final String valueAlias;

  @JsonCreator
  public ExplodeMapSpec(
      @JsonProperty("column") List<String> column,
      @JsonProperty("keyAlias") String keyAlias,
      @JsonProperty("valueAlias") String valueAlias
  )
  {
    this.columns = column;
    this.keyAlias = keyAlias;
    this.valueAlias = valueAlias;
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(column), "'columns' cannot be null or empty");
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public String getKeyAlias()
  {
    return keyAlias;
  }

  @JsonProperty
  public String getValueAlias()
  {
    return valueAlias;
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
    final ExplodeMapSpec other = (ExplodeMapSpec) o;
    return columns.equals(other.columns) &&
           Objects.equals(keyAlias, other.keyAlias) &&
           Objects.equals(valueAlias, other.valueAlias);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columns, keyAlias, valueAlias);
  }

  @Override
  public String toString()
  {
    return "ExplodeMapSpec{columns=" + columns + ", keyAlias=" + keyAlias + ", valueAlias=" + valueAlias + '}';
  }

  @Override
  public Function<Map<String, Object>, Iterable<Map<String, Object>>> prepare()
  {
    return new Function<Map<String, Object>, Iterable<Map<String, Object>>>()
    {
      private final String[] keys = columns.toArray(new String[0]);
      private final Object[] values = new Object[keys.length][2];
      private final List<Map<String, Object>> events = Lists.newArrayList();

      @Override
      @SuppressWarnings("unchecked")
      public Iterable<Map<String, Object>> apply(final Map<String, Object> input)
      {
        events.clear();
        for (int i = 0; i < keys.length; i++) {
          values[i] = input.remove(keys[i]);
        }
        for (int i = 0; i < keys.length; i++) {
          Map<String, Object> copy = Maps.newHashMap(input);
          if (keyAlias != null) {
            copy.put(keyAlias, keys[i]);
          }
          copy.put(valueAlias != null ? valueAlias : keys[i], values[i]);
          events.add(copy);
        }
        return events;
      }
    };
  }

  @Override
  public List<String> resolve(List<String> schema)
  {
    if (keyAlias != null && !schema.contains(keyAlias)) {
      schema = GuavaUtils.concat(schema, keyAlias);
    }
    if (valueAlias != null && !schema.contains(valueAlias)) {
      schema = GuavaUtils.concat(schema, valueAlias);
    }
    return schema;
  }

  @Override
  public Schema resolve(Query query, Schema schema, ObjectMapper mapper)
  {
    ValueDesc merged = null;
    for (String column : columns) {
      merged = ValueDesc.toCommonType(merged, schema.resolve(column));
    }
    if (keyAlias != null) {
      schema = schema.appendMetrics(keyAlias, ValueDesc.STRING);
    }
    if (valueAlias != null) {
      schema = schema.appendMetrics(valueAlias, merged);
    }
    return schema;
  }
}
