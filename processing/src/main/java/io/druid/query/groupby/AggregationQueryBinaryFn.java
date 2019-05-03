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

package io.druid.query.groupby;

import com.google.common.annotations.VisibleForTesting;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Row;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;

public class AggregationQueryBinaryFn implements BinaryFn<Row, Row, Row>
{
  private final int start;
  private final AggregatorFactory.Combiner[] combiners;

  public AggregationQueryBinaryFn(BaseAggregationQuery query)
  {
    start = query.getDimensions().size() + 1;
    combiners = AggregatorFactory.toCombinerArray(query.getAggregatorSpecs());
  }

  @VisibleForTesting
  public AggregationQueryBinaryFn(List<AggregatorFactory> aggregatorFactories)
  {
    start = 0;
    combiners = AggregatorFactory.toCombinerArray(aggregatorFactories);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Row apply(final Row arg1, final Row arg2)
  {
    if (arg1 == null) {
      return arg2;
    } else if (arg2 == null) {
      return arg1;
    }
    final Object[] values1 = ((CompactRow) arg1).getValues();
    final Object[] values2 = ((CompactRow) arg2).getValues();
    int index = start;
    for (AggregatorFactory.Combiner combiner : combiners) {
      values1[index] = combiner.combine(values1[index], values2[index]);
      index++;
    }
    return arg1;
  }
}