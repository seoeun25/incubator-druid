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

package io.druid.segment.incremental;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.Row;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AbstractArrayAggregatorFactory;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OnheapIncrementalIndex extends IncrementalIndex<Aggregator>
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);

  protected final ConcurrentMap<TimeAndDims, Integer> facts;
  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
  private final AtomicInteger indexIncrement = new AtomicInteger(0);
  private ColumnSelectorFactory[] selectors;

  private final int[] arrayAggregatorIndices;

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean sortFacts,
      boolean estimate,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, sortFacts, estimate, maxRowCount);

    if (sortFacts) {
      this.facts = new ConcurrentSkipListMap<>(dimsComparator());
    } else {
      this.facts = new ConcurrentHashMap<>();
    }
    List<Integer> arrayAggregatorIndices = Lists.newArrayList();
    final AggregatorFactory[] metrics = getMetricAggs();
    for (int i = 0; i < metrics.length; i++) {
      if (metrics[i] instanceof AbstractArrayAggregatorFactory) {
        arrayAggregatorIndices.add(i);
      }
    }
    this.arrayAggregatorIndices = Ints.toArray(arrayAggregatorIndices);
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      Granularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean sortFacts,
      boolean rollup,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .withRollup(rollup)
                                            .build(),
        deserializeComplexMetrics,
        reportParseExceptions,
        sortFacts,
        false,
        maxRowCount
    );
  }

  @VisibleForTesting
  public OnheapIncrementalIndex(
      long minTimestamp, Granularity gran, final AggregatorFactory[] metrics, int maxRowCount
  )
  {
    this(minTimestamp, gran, true, metrics, maxRowCount);
  }

  @VisibleForTesting
  public OnheapIncrementalIndex(
      long minTimestamp,
      Granularity gran,
      boolean rollup,
      final AggregatorFactory[] metrics,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .withRollup(rollup)
                                            .build(),
        true,
        true,
        true,
        true,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean reportParseExceptions,
      int maxRowCount
  )
  {
    this(incrementalIndexSchema, true, reportParseExceptions, true, true, maxRowCount);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterable<Map.Entry<TimeAndDims, Integer>> getRangeOf(final long from, final long to, Boolean timeDescending)
  {
    return getFacts(facts, from, to, timeDescending);
  }

  @Override
  protected Aggregator[] initAggs(
      AggregatorFactory[] metrics, Supplier<Row> rowSupplier, boolean deserializeComplexMetrics
  )
  {
    this.selectors = new ColumnSelectorFactory[metrics.length];
    for (int i = 0; i < metrics.length; i++) {
      ColumnSelectorFactory delegate = new ColumnSelectorFactories.FromInputRow(
          rowSupplier,
          metrics[i],
          deserializeComplexMetrics
      );
      selectors[i] = new ColumnSelectorFactories.Caching(delegate);
    }

    return new Aggregator[metrics.length];
  }

  @Override
  protected Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      Row row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<Row> rowContainer,
      Supplier<Row> rowSupplier
  ) throws IndexSizeExceededException
  {
    final Integer priorIndex = facts.get(key);

    final Aggregator[] aggs;

    rowContainer.set(row);
    if (null != priorIndex) {
      aggs = concurrentGet(priorIndex);
      doAggregate(aggs, reportParseExceptions);
    } else {
      aggs = factorizeAggs(metrics, reportParseExceptions);

      final Integer rowIndex = indexIncrement.getAndIncrement();
      concurrentSet(rowIndex, aggs);

      // Last ditch sanity checks
      if (numEntries.get() >= maxRowCount && !facts.containsKey(key)) {
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
      final Integer prev = facts.putIfAbsent(key, rowIndex);
      if (null == prev) {
        numEntries.incrementAndGet();
      } else {
        // We lost a race
        doAggregate(concurrentGet(prev), reportParseExceptions);
        // Free up the misfire
        concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }
    rowContainer.set(null);
    return numEntries.get();
  }

  private Aggregator[] factorizeAggs(AggregatorFactory[] metrics, boolean reportParseExceptions)
  {
    final Aggregator[] aggs = new Aggregator[metrics.length];
    for (int i = 0; i < metrics.length; i++) {
      aggregate(aggs[i] = metrics[i].factorize(selectors[i]), reportParseExceptions);
    }
    return aggs;
  }

  private void doAggregate(Aggregator[] aggs, boolean reportParseExceptions)
  {
    for (Aggregator agg : aggs) {
      aggregate(agg, reportParseExceptions);
    }
  }

  private void aggregate(Aggregator agg, boolean reportParseExceptions)
  {
    try {
      agg.aggregate();
    }
    catch (ParseException e) {
      // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
      if (reportParseExceptions) {
        throw new ParseException(e, "Encountered parse error for aggregator[%s]", agg);
      } else {
        log.debug(e, "Encountered parse error, skipping aggregator[%s].", agg);
      }
    }
  }

  protected Aggregator[] concurrentGet(int offset)
  {
    // All get operations should be fine
    return aggregators.get(offset);
  }

  protected void concurrentSet(int offset, Aggregator[] value)
  {
    aggregators.put(offset, value);
  }

  protected void concurrentRemove(int offset)
  {
    aggregators.remove(offset);
  }

  @Override
  public long estimatedOccupation()
  {
    long estimation = super.estimatedOccupation();
    if (arrayAggregatorIndices.length > 0) {
      for (Aggregator[] array : aggregators.values()) {
        for (int index : arrayAggregatorIndices) {
          estimation += ((Aggregators.EstimableAggregator)array[index]).estimateOccupation();
        }
      }
    }
    return estimation;
  }

  @Override
  protected Aggregator[] getAggsForRow(int rowOffset)
  {
    return concurrentGet(rowOffset);
  }

  @Override
  protected Object getAggVal(Aggregator agg, int rowOffset, int aggPosition)
  {
    return agg.get();
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getFloat();
  }

  @Override
  protected double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getDouble();
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getLong();
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].get();
  }

  /**
   * Clear out maps to allow GC
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    aggregators.clear();
    facts.clear();
    if (selectors != null) {
      Arrays.fill(selectors, null);
    }
  }
}
