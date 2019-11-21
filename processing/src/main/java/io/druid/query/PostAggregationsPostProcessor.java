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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.select.Schema;

import java.util.List;
import java.util.Map;

/**
 */
public class PostAggregationsPostProcessor extends PostProcessingOperator.ReturnsRow<Row>
    implements Schema.SchemaResolving
{
  private final List<PostAggregator> postAggregations;

  @JsonCreator
  public PostAggregationsPostProcessor(
      @JsonProperty("postAggregations") List<PostAggregator> postAggregations
  )
  {
    this.postAggregations = postAggregations == null ? ImmutableList.<PostAggregator>of() : postAggregations;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<PostAggregator> getPostAggregations()
  {
    return postAggregations;
  }

  @Override
  public QueryRunner<Row> postProcess(final QueryRunner<Row> baseRunner)
  {
    if (postAggregations.isEmpty()) {
      return baseRunner;
    }
    return new QueryRunner<Row>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Row> run(Query query, Map responseContext)
      {
        final Sequence<Row> sequence = Queries.convertToRow(query, baseRunner.run(query, responseContext));
        return Queries.convertBack(
            query, Sequences.map(
                sequence, new Function<Row, Row>()
                {
                  @Override
                  public Row apply(Row input)
                  {
                    Map<String, Object> event = Rows.asMap(input);
                    for (PostAggregator postAggregator : postAggregations) {
                      event.put(postAggregator.getName(), postAggregator.compute(input.getTimestamp(), event));
                    }
                    return new MapBasedRow(input.getTimestamp(), event);
                  }
                }
            )
        );
      }
    };
  }

  @Override
  public List<String> resolve(List<String> schema)
  {
    if (GuavaUtils.isNullOrEmpty(postAggregations)) {
      return schema;
    }
    for (PostAggregator postAggregator : postAggregations) {
      String outputName = postAggregator.getName();
      if (!schema.contains(outputName)) {
        schema.add(outputName);
      }
    }
    return schema;
  }

  @Override
  public Schema resolve(Query query, Schema schema, ObjectMapper mapper)
  {
    if (GuavaUtils.isNullOrEmpty(postAggregations)) {
      return schema;
    }
    List<String> dimensionNames = Lists.newArrayList(schema.getDimensionNames());
    List<String> metricNames = Lists.newArrayList(schema.getMetricNames());
    List<ValueDesc> types = Lists.newArrayList(schema.getColumnTypes());
    for (PostAggregator postAggregator : postAggregations) {
      String outputName = postAggregator.getName();
      ValueDesc valueDesc = postAggregator.resolve(schema);
      int index = dimensionNames.indexOf(outputName);
      if (index >= 0) {
        types.set(index, valueDesc);
        continue;
      }
      index = metricNames.indexOf(outputName);
      if (index >= 0) {
        types.set(dimensionNames.size() + index, valueDesc);
        continue;
      }
      metricNames.add(outputName);
      types.add(valueDesc);
    }
    return new Schema(dimensionNames, metricNames, types);
  }

  @Override
  public String toString()
  {
    return "PostAggregationsPostProcessor{" +
           "postAggregations=" + postAggregations +
           '}';
  }
}
