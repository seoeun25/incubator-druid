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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;

import java.util.List;
import java.util.Map;

/**
 */
public class ArrayToMap extends PostProcessingOperator.Abstract<Row>
{
  private final List<String> columnNames;

  @JsonCreator
  public ArrayToMap(@JsonProperty("columnNames") List<String> columnNames)
  {
    this.columnNames = columnNames;
  }

  @JsonProperty
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner<Row> baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        return Sequences.map(baseRunner.run(query, responseContext), new Function<Object[], Map>()
        {
          @Override
          public Map apply(Object[] input)
          {
            final int limit = Math.min(input.length, columnNames.size());
            final Map<String, Object> map = Maps.newHashMapWithExpectedSize(limit);
            for (int i = 0; i < limit; i++) {
              map.put(columnNames.get(i), input[i]);
            }
            return map;
          }
        });
      }
    };
  }

  @Override
  public boolean hasTabularOutput()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "ArrayToMap{" +
           "columnNames=" + columnNames +
           '}';
  }
}