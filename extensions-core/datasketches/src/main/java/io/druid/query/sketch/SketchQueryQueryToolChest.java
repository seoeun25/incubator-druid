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

package io.druid.query.sketch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.yahoo.memory.NativeMemory;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.datasketches.theta.SketchOperations;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class SketchQueryQueryToolChest extends QueryToolChest<Result<Map<String, Object>>, SketchQuery>
{
  private static final TypeReference<Result<Map<String, Object>>> TYPE_REFERENCE =
      new TypeReference<Result<Map<String, Object>>>()
      {
      };

  private static final Logger logger = new Logger(SketchQueryQueryToolChest.class);

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public SketchQueryQueryToolChest(
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Result<Map<String, Object>>> mergeResults(
      QueryRunner<Result<Map<String, Object>>> runner
  )
  {
    return new ResultMergeQueryRunner<Result<Map<String, Object>>>(runner)
    {
      @Override
      protected Ordering<Result<Map<String, Object>>> makeOrdering(Query<Result<Map<String, Object>>> query)
      {
        return ResultGranularTimestampComparator.create(
            QueryGranularities.ALL,
            query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<Map<String, Object>>, Result<Map<String, Object>>, Result<Map<String, Object>>>
      createMergeFn(Query<Result<Map<String, Object>>> input)
      {
        final SketchQuery sketch = (SketchQuery) input;
        return new SketchBinaryFn(sketch.getSketchParam(), sketch.getSketchOp().handler());
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SketchQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<Result<Map<String, Object>>, Result<Map<String, Object>>> makePreComputeManipulatorFn(
      final SketchQuery query, MetricManipulationFn fn
  )
  {
    // fn is for aggregators.. we don't need to apply it
    return new Function<Result<Map<String, Object>>, Result<Map<String, Object>>>()
    {
      @Override
      public Result<Map<String, Object>> apply(Result<Map<String, Object>> input)
      {
        Map<String, Object> sketches = input.getValue();
        for (Map.Entry<String, Object> entry : sketches.entrySet()) {
          byte[] value = SketchOperations.asBytes(entry.getValue());
          ValueType type = TypedSketch.fromByte(value[0]);
          NativeMemory memory = new NativeMemory(Arrays.copyOfRange(value, 1, value.length));
          Object deserialize;
          if (query.getSketchOp() == SketchOp.THETA) {
            deserialize = SketchOperations.deserializeFromMemory(memory);
          } else if (query.getSketchOp() == SketchOp.QUANTILE) {
            deserialize = SketchOperations.deserializeQuantileFromMemory(memory, type);
          } else if (query.getSketchOp() == SketchOp.FREQUENCY) {
            deserialize = SketchOperations.deserializeFrequencyFromMemory(memory, type);
          } else {
            deserialize = SketchOperations.deserializeSamplingFromMemory(memory, type);
          }
          entry.setValue(TypedSketch.of(type, deserialize));
        }
        return input;
      }
    };
  }

  @Override
  public Function<Result<Map<String, Object>>, Result<Map<String, Object>>> makePostComputeManipulatorFn(
      SketchQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<Map<String, Object>>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public <T> CacheStrategy<Result<Map<String, Object>>, T, SketchQuery> getCacheStrategy(SketchQuery query)
  {
    return null;  //todo
  }

  @Override
  public QueryRunner<Result<Map<String, Object>>> preMergeQueryDecoration(
      QueryRunner<Result<Map<String, Object>>> runner
  )
  {
    return intervalChunkingQueryRunnerDecorator.decorate(runner, this);
  }

  @Override
  public <I> QueryRunner<Result<Map<String, Object>>> handleSubQuery(
      final QueryRunner<I> subQueryRunner,
      final QuerySegmentWalker segmentWalker,
      final ExecutorService executor,
      final int maxRowCount
  )
  {
    return new QueryRunner<Result<Map<String, Object>>>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Result<Map<String, Object>>> run(
          Query<Result<Map<String, Object>>> query,
          final Map<String, Object> responseContext
      )
      {
        final Query<I> subQuery = ((QueryDataSource)query.getDataSource()).getQuery();
        final IncrementalIndexSchema schema = Queries.relaySchema(subQuery, segmentWalker);
        final Sequence<Row> innerSequence = Sequences.map(
            subQueryRunner.run(subQuery, responseContext), Queries.getRowConverter(subQuery)
        );

        logger.info(
            "Accumulating into intermediate index with dimension %s and metric %s",
            schema.getDimensionsSpec().getDimensionNames(),
            schema.getMetricNames()
        );
        long start = System.currentTimeMillis();
        final IncrementalIndex innerQueryResultIndex = innerSequence.accumulate(
            new OnheapIncrementalIndex(schema, false, true, true, maxRowCount),
            GroupByQueryHelper.<Row>newIndexAccumulator()
        );
        logger.info(
            "Accumulated sub-query into index in %,d msec.. total %,d rows",
            (System.currentTimeMillis() - start),
            innerQueryResultIndex.size()
        );
        if (innerQueryResultIndex.isEmpty()) {
          return Sequences.empty();
        }

        List<String> dataSources = query.getDataSource().getNames();
        if (dataSources.size() > 1) {
          query = query.withDataSource(
              new TableDataSource(org.apache.commons.lang.StringUtils.join(dataSources, '_'))
          );
        }
        final String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());
        final SketchQuery outerQuery = (SketchQuery) query;

        IncrementalIndexStorageAdapter index = new IncrementalIndexStorageAdapter.Temporary(
            dataSource, innerQueryResultIndex
        );
        final SketchQueryRunner runner = new SketchQueryRunner(
            new IncrementalIndexSegment(innerQueryResultIndex, index.getSegmentIdentifier())
        );
        return new ResourceClosingSequence<>(
            Sequences.concat(
                Sequences.map(
                    Sequences.simple(outerQuery.getIntervals()),
                    new Function<Interval, Sequence<Result<Map<String, Object>>>>()
                    {
                      @Override
                      public Sequence<Result<Map<String, Object>>> apply(Interval interval)
                      {
                        return runner.run(
                            outerQuery.withQuerySegmentSpec(
                                new MultipleIntervalSegmentSpec(ImmutableList.of(interval))
                            ),
                            responseContext
                        );
                      }
                    }
                )
            ), innerQueryResultIndex
        );
      }
    };
  }
}