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

package io.druid.query.aggregation.covariance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.MetricValueExtractor;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("stats.covariance")
public class CovariancePostProcessor implements PostProcessingOperator
{
  @JsonCreator
  public CovariancePostProcessor() { }

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        Preconditions.checkArgument(query instanceof TimeseriesQuery);
        List<Result<TimeseriesResultValue>> rows = Sequences.toList(
            baseRunner.run(query, Maps.<String, Object>newHashMap()),
            Lists.<Result<TimeseriesResultValue>>newArrayList()
        );
        if (rows.isEmpty()) {
          return Sequences.empty();
        }
        Result<TimeseriesResultValue> result = Iterables.getOnlyElement(rows);
        List<Pair<Double, String>> covariances = Lists.newArrayList();
        MetricValueExtractor row = result.getValue();
        for (String column : row.getColumns()) {
          covariances.add(Pair.of(row.getDoubleMetric(column), column));
        }
        Collections.sort(
            covariances,
            Pair.lhsComparator(
                Ordering.<Double>natural().onResultOf(
                    new Function<Double, Double>()
                    {
                      @Override
                      public Double apply(Double input) { return Math.abs(input); }
                    }
                ).reverse()
            )
        );
        List<Map<String, Object>> covarianceBest = Lists.newArrayList();
        for (int i = 0; i < Math.min(covariances.size(), 5); i++) {
          Pair<Double, String> covariance = covariances.get(i);
          covarianceBest.add(
              ImmutableMap.<String, Object>of(
                  "with", covariance.rhs,
                  "covariance", covariance.lhs
              )
          );
        }
        return Sequences.simple(covarianceBest);
      }
    };
  }

  @Override
  public boolean supportsUnionProcessing()
  {
    return false;
  }

  @Override
  public boolean hasTabularOutput()
  {
    return true;
  }
}