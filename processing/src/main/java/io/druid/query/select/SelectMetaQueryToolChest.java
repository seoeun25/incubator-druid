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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.Granularity;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class SelectMetaQueryToolChest extends QueryToolChest<Result<SelectMetaResultValue>, SelectMetaQuery>
{
  private static final TypeReference<Result<SelectMetaResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<SelectMetaResultValue>>()
      {
      };

  @Override
  public QueryRunner<Result<SelectMetaResultValue>> mergeResults(QueryRunner<Result<SelectMetaResultValue>> runner)
  {
    return new ResultMergeQueryRunner<Result<SelectMetaResultValue>>(runner)
    {
      @Override
      protected Ordering<Result<SelectMetaResultValue>> makeOrdering(Query<Result<SelectMetaResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(
            ((SelectMetaQuery) query).getGranularity(), query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<SelectMetaResultValue>, Result<SelectMetaResultValue>, Result<SelectMetaResultValue>> createMergeFn(
          Query<Result<SelectMetaResultValue>> input
      )
      {
        SelectMetaQuery query = (SelectMetaQuery) input;
        final Granularity gran = query.getGranularity();
        return new BinaryFn<Result<SelectMetaResultValue>, Result<SelectMetaResultValue>, Result<SelectMetaResultValue>>()
        {
          @Override
          public Result<SelectMetaResultValue> apply(
              Result<SelectMetaResultValue> arg1, Result<SelectMetaResultValue> arg2
          )
          {
            if (arg1 == null) {
              return arg2;
            }
            if (arg2 == null) {
              return arg1;
            }
            final DateTime timestamp = gran instanceof AllGranularity
                                       ? arg1.getTimestamp()
                                       : gran.bucketStart(arg1.getTimestamp());
            SelectMetaResultValue value1 = arg1.getValue();
            SelectMetaResultValue value2 = arg2.getValue();

            Map<String, Integer> merged = Maps.newTreeMap();
            merged.putAll(value1.getPerSegmentCounts());
            for (Map.Entry<String, Integer> entry : value2.getPerSegmentCounts().entrySet()) {
              Integer prev = merged.get(entry.getKey());
              merged.put(entry.getKey(), prev == null ? entry.getValue() : prev + entry.getValue());
            }
            Schema mergedSchema = value1.getSchema().merge(value2.getSchema());
            long estimatedSize = value1.getEstimatedSize() + value2.getEstimatedSize();
            return new Result<>(
                timestamp,
                new SelectMetaResultValue(mergedSchema, merged, estimatedSize)
            );
          }
        };
      }
    };
  }


  @Override
  public <T extends LogicalSegment> List<T> filterSegments(SelectMetaQuery query, List<T> segments)
  {
    // shares same logic
    return SelectQueryQueryToolChest.filterSegmentsOnPagingSpec(query.toBaseQuery(), segments);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SelectMetaQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<Result<SelectMetaResultValue>, Result<SelectMetaResultValue>> makePreComputeManipulatorFn(
      SelectMetaQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<SelectMetaResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }
}