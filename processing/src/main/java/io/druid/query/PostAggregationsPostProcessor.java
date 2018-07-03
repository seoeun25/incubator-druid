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
import com.google.common.collect.ImmutableList;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.aggregation.PostAggregator;

import java.util.List;
import java.util.Map;

/**
 */
public class PostAggregationsPostProcessor extends PostProcessingOperator.Abstract
{
  private final List<PostAggregator> postAggregations;

  @JsonCreator
  public PostAggregationsPostProcessor(
      @JsonProperty("postAggregations") List<PostAggregator> postAggregations
  )
  {
    this.postAggregations = postAggregations == null ? ImmutableList.<PostAggregator>of() : postAggregations;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    if (postAggregations.isEmpty()) {
      return baseRunner;
    }
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
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
}