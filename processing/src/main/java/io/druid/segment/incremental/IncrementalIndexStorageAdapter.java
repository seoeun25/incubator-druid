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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.cache.Cache;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularity;
import io.druid.query.QueryInterruptedException;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.Metadata;
import io.druid.segment.NullDimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.SingleScanTimeDimSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class IncrementalIndexStorageAdapter implements StorageAdapter
{
  private static final NullDimensionSelector NULL_DIMENSION_SELECTOR = new NullDimensionSelector(String.class);

  private final String segmentIdentifier;
  private final IncrementalIndex<?> index;

  public IncrementalIndexStorageAdapter(String segmentIdentifier, IncrementalIndex<?> index)
  {
    this.segmentIdentifier = segmentIdentifier;
    this.index = index;
  }

  public IncrementalIndexStorageAdapter(
      IncrementalIndex<?> index
  )
  {
    this(null, index);
  }

  @Override
  public String getSegmentIdentifier()
  {
    if (segmentIdentifier == null) {
      throw new UnsupportedOperationException();
    }
    return segmentIdentifier;
  }

  @Override
  public Interval getInterval()
  {
    return index.getInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<String>(index.getDimensionNames(), String.class);
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return index.getMetricNames();
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    if (dimension.equals(Column.TIME_COLUMN_NAME)) {
      return Integer.MAX_VALUE;
    }
    IncrementalIndex.DimDim dimDim = index.getDimensionValues(dimension);
    if (dimDim == null) {
      return 0;
    }
    return dimDim.size();
  }

  @Override
  public int getNumRows()
  {
    return index.size();
  }

  @Override
  public DateTime getMinTime()
  {
    return index.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return index.getMaxTime();
  }

  @Override
  public Comparable getMinValue(String column)
  {
    IncrementalIndex.DimDim dimDim = index.getDimensionValues(column);
    return dimDim == null ? null : dimDim.getMinValue();
  }

  @Override
  public Comparable getMaxValue(String column)
  {
    IncrementalIndex.DimDim dimDim = index.getDimensionValues(column);
    return dimDim == null ? null : dimDim.getMaxValue();
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(false).build();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return index.getCapabilities(column);
  }

  @Override
  public ValueDesc getColumnType(String column)
  {
    // check first for compatibility
    ValueDesc metricType = index.getMetricType(column);
    if (metricType != null) {
      return metricType;
    }
    IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(column);
    if (dimensionDesc != null) {
      return ValueDesc.ofDimension(dimensionDesc.getCapabilities().getType());
    }
    ColumnCapabilities capabilities = index.getCapabilities(column);
    if (capabilities != null) {
      return ValueDesc.of(capabilities.getType());
    }
    return null;
  }

  @Override
  public long getSerializedSize(String column)
  {
    return 0L;
  }

  @Override
  public float getAverageSize(String column)
  {
    IncrementalIndex.DimensionDesc dimDesc = index.getDimension(column);
    if (dimDesc != null) {
      IncrementalIndex.DimDim values = dimDesc.getValues();
      return (values.estimatedSize() - Ints.BYTES * index.ingestedRows()) / values.size();
    }
    IncrementalIndex.MetricDesc metricDesc = index.getMetricDesc(column);
    if (metricDesc != null) {
      switch (metricDesc.getCapabilities().getType()) {
        case FLOAT: return Floats.BYTES;
        case LONG: return Longs.BYTES;
        case DOUBLE: return Doubles.BYTES;
        // ?
      }
    }
    return 0;
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return index.getMaxIngestedEventTime();
  }

  @Override
  public Sequence<Cursor> makeCursors(
      final DimFilter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final Granularity gran,
      final Cache cache,
      final boolean descending
  )
  {
    if (index.isEmpty()) {
      return Sequences.empty();
    }

    final Interval dataInterval = new Interval(getMinTime(), gran.bucketEnd(getMaxTime()));

    if (!interval.overlaps(dataInterval)) {
      return Sequences.empty();
    }

    virtualColumns.addImplicitVCs(this);

    final Interval actualInterval = interval.overlap(dataInterval);

    Iterable<Interval> iterable = gran.getIterable(actualInterval);
    if (descending) {
      iterable = Lists.reverse(ImmutableList.copyOf(iterable));
    }

    final RowResolver resolver = new RowResolver(this, virtualColumns);

    return Sequences.map(
        Sequences.simple(iterable),
        new Function<Interval, Cursor>()
        {
          private EntryHolder currEntry = new EntryHolder();

          @Override
          public Cursor apply(final Interval interval)
          {
            return new Cursor.ExprSupport()
            {
              private Iterator<Map.Entry<IncrementalIndex.TimeAndDims, Integer>> baseIter;
              private Iterable<Map.Entry<IncrementalIndex.TimeAndDims, Integer>> cursorMap;
              private final DateTime time;
              private int numAdvanced = -1;
              private boolean done;

              private final ValueMatcher filterMatcher;
              {
                long timeStart = Math.max(interval.getStartMillis(), actualInterval.getStartMillis());
                long timeEnd = Math.min(gran.increment(interval.getStart()).getMillis(), actualInterval.getEndMillis());
                if (timeEnd == dataInterval.getEndMillis()) {
                  timeEnd = timeEnd + 1;    // inclusive
                }
                cursorMap = index.getRangeOf(timeStart, timeEnd, descending);
                time = gran.toDateTime(interval.getStartMillis());
                filterMatcher = filter == null ? BooleanValueMatcher.TRUE : filter.toFilter().makeMatcher(this);
                reset();
              }

              @Override
              public DateTime getTime()
              {
                return time;
              }

              @Override
              public void advance()
              {
                if (!baseIter.hasNext()) {
                  done = true;
                  return;
                }

                while (baseIter.hasNext()) {
                  if (Thread.interrupted()) {
                    throw new QueryInterruptedException(new InterruptedException());
                  }

                  currEntry.set(baseIter.next());

                  if (filterMatcher.matches()) {
                    return;
                  }
                }

                if (!filterMatcher.matches()) {
                  done = true;
                }
              }

              @Override
              public void advanceTo(int offset)
              {
                int count = 0;
                while (count < offset && !isDone()) {
                  advance();
                  count++;
                }
              }

              @Override
              public boolean isDone()
              {
                return done;
              }

              @Override
              public void reset()
              {
                baseIter = cursorMap.iterator();

                if (numAdvanced == -1) {
                  numAdvanced = 0;
                } else {
                  Iterators.advance(baseIter, numAdvanced);
                }

                if (Thread.interrupted()) {
                  throw new QueryInterruptedException(new InterruptedException());
                }

                boolean foundMatched = false;
                while (baseIter.hasNext()) {
                  currEntry.set(baseIter.next());
                  if (filterMatcher.matches()) {
                    foundMatched = true;
                    break;
                  }

                  numAdvanced++;
                }

                done = !foundMatched && !baseIter.hasNext();
              }

              @Override
              public Iterable<String> getColumnNames()
              {
                return Iterables.concat(index.getDimensionNames(), index.getMetricNames());
              }

              @Override
              public DimensionSelector makeDimensionSelector(
                  DimensionSpec dimensionSpec
              )
              {
                return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
              }

              private DimensionSelector makeDimensionSelectorUndecorated(
                  DimensionSpec dimensionSpec
              )
              {
                final String dimension = dimensionSpec.getDimension();
                final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

                if (dimension.equals(Column.TIME_COLUMN_NAME)) {
                  LongColumnSelector selector = makeLongColumnSelector(dimension);
                  if (extractionFn != null) {
                    return new SingleScanTimeDimSelector(selector, extractionFn, descending);
                  }
                  return VirtualColumns.toDimensionSelector(selector);
                }

                final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimension);
                if (dimensionDesc == null) {
                  VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(dimension);
                  if (virtualColumn != null) {
                    return virtualColumn.asDimension(dimension, extractionFn, this);
                  }
                  if (index.getMetricIndex(dimension) != null) {
                    // todo: group-by columns are converted to string
                    return VirtualColumns.toDimensionSelector(makeObjectColumnSelector(dimension), extractionFn);
                  }
                  return NULL_DIMENSION_SELECTOR;
                }

                final int dimIndex = dimensionDesc.getIndex();
                final IncrementalIndex.DimDim dimValLookup = dimensionDesc.getValues();

                final int maxId = dimValLookup.size();

                return new DimensionSelector()
                {
                  @Override
                  public IndexedInts getRow()
                  {
                    final int[][] dims = currEntry.getKey().getDims();

                    int[] indices = dimIndex < dims.length ? dims[dimIndex] : null;

                    List<Integer> valsTmp = null;
                    if ((indices == null || indices.length == 0) && dimValLookup.contains(null)) {
                      int id = dimValLookup.getId(null);
                      if (id < maxId) {
                        valsTmp = new ArrayList<>(1);
                        valsTmp.add(id);
                      }
                    } else if (indices != null && indices.length > 0) {
                      valsTmp = new ArrayList<>(indices.length);
                      for (int i = 0; i < indices.length; i++) {
                        int id = indices[i];
                        if (id < maxId) {
                          valsTmp.add(id);
                        }
                      }
                    }

                    final List<Integer> vals = valsTmp == null ? Collections.<Integer>emptyList() : valsTmp;
                    return new IndexedInts()
                    {
                      @Override
                      public int size()
                      {
                        return vals.size();
                      }

                      @Override
                      public int get(int index)
                      {
                        return vals.get(index);
                      }

                      @Override
                      public Iterator<Integer> iterator()
                      {
                        return vals.iterator();
                      }

                      @Override
                      public void fill(int index, int[] toFill)
                      {
                        throw new UnsupportedOperationException("fill not supported");
                      }

                      @Override
                      public void close() throws IOException
                      {

                      }
                    };
                  }

                  @Override
                  public int getValueCardinality()
                  {
                    return maxId;
                  }

                  @Override
                  public Comparable lookupName(int id)
                  {
                    // TODO: needs update to DimensionSelector interface to allow multi-types, just use Strings for now
                    final Comparable value = dimValLookup.getValue(id);
                    final String strValue = value == null ? null : value.toString();
                    return extractionFn == null ? strValue : extractionFn.apply(strValue);
                  }

                  @Override
                  public Class type()
                  {
                    return String.class;
                  }

                  @Override
                  public int lookupId(Comparable name)
                  {
                    if (extractionFn != null) {
                      throw new UnsupportedOperationException(
                          "cannot perform lookup when applying an extraction function"
                      );
                    }
                    return dimValLookup.getId(name);
                  }
                };
              }

              @Override
              public FloatColumnSelector makeFloatColumnSelector(String columnName)
              {
                final Integer metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt == null) {
                  VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(columnName);
                  if (virtualColumn != null) {
                    return virtualColumn.asFloatMetric(columnName, this);
                  }
                  return new FloatColumnSelector()
                  {
                    @Override
                    public float get()
                    {
                      return 0.0f;
                    }
                  };
                }

                final int metricIndex = metricIndexInt;
                return new FloatColumnSelector()
                {
                  @Override
                  public float get()
                  {
                    return index.getMetricFloatValue(currEntry.getValue(), metricIndex);
                  }
                };
              }

              @Override
              public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
              {
                final Integer metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt == null) {
                  VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(columnName);
                  if (virtualColumn != null) {
                    return virtualColumn.asDoubleMetric(columnName, this);
                  }
                  return new DoubleColumnSelector()
                  {
                    @Override
                    public double get()
                    {
                      return 0.0d;
                    }
                  };
                }

                final int metricIndex = metricIndexInt;
                return new DoubleColumnSelector()
                {
                  @Override
                  public double get()
                  {
                    return index.getMetricDoubleValue(currEntry.getValue(), metricIndex);
                  }
                };
              }

              @Override
              public LongColumnSelector makeLongColumnSelector(String columnName)
              {
                if (columnName.equals(Column.TIME_COLUMN_NAME)) {
                  return new LongColumnSelector()
                  {
                    @Override
                    public long get()
                    {
                      return currEntry.getKey().getTimestamp();
                    }
                  };
                }
                final Integer metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt == null) {
                  VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(columnName);
                  if (virtualColumn != null) {
                    return virtualColumn.asLongMetric(columnName, this);
                  }
                  return new LongColumnSelector()
                  {
                    @Override
                    public long get()
                    {
                      return 0L;
                    }
                  };
                }

                final int metricIndex = metricIndexInt;

                return new LongColumnSelector()
                {
                  @Override
                  public long get()
                  {
                    return index.getMetricLongValue(
                        currEntry.getValue(),
                        metricIndex
                    );
                  }
                };
              }

              @Override
              public ObjectColumnSelector makeObjectColumnSelector(String column)
              {
                if (column.equals(Column.TIME_COLUMN_NAME)) {
                  return new ObjectColumnSelector<Long>()
                  {
                    @Override
                    public ValueDesc type()
                    {
                      return ValueDesc.LONG;
                    }

                    @Override
                    public Long get()
                    {
                      return currEntry.getKey().getTimestamp();
                    }
                  };
                }

                final Integer metricIndexInt = index.getMetricIndex(column);
                if (metricIndexInt != null) {
                  final int metricIndex = metricIndexInt;
                  final ValueDesc valueType = index.getMetricType(column);
                  return new ObjectColumnSelector()
                  {
                    @Override
                    public ValueDesc type()
                    {
                      return valueType;
                    }

                    @Override
                    public Object get()
                    {
                      return index.getMetricObjectValue(
                          currEntry.getValue(),
                          metricIndex
                      );
                    }
                  };
                }

                IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(column);

                if (dimensionDesc == null) {
                  VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(column);
                  if (virtualColumn != null) {
                    return virtualColumn.asMetric(column, this);
                  }
                  return null;
                }

                final ColumnCapabilitiesImpl capabilities = dimensionDesc.getCapabilities();
                final ValueDesc valueType = capabilities.hasMultipleValues()
                                            ? ValueDesc.ofMultiValued(capabilities.getType())
                                            : ValueDesc.of(capabilities.getType());

                final int dimensionIndex = dimensionDesc.getIndex();
                final IncrementalIndex.DimDim dimDim = dimensionDesc.getValues();

                return new ObjectColumnSelector<Object>()
                {
                  @Override
                  public ValueDesc type()
                  {
                    return valueType;
                  }

                  @Override
                  public Object get()
                  {
                    IncrementalIndex.TimeAndDims key = currEntry.getKey();
                    if (key == null) {
                      return null;
                    }

                    int[][] dims = key.getDims();
                    if (dimensionIndex >= dims.length) {
                      return null;
                    }

                    final int[] dimIdx = dims[dimensionIndex];
                    if (dimIdx == null || dimIdx.length == 0) {
                      return null;
                    }
                    if (dimIdx.length == 1) {
                      return dimDim.getValue(dimIdx[0]);
                    }
                    Comparable[] dimVals = new Comparable[dimIdx.length];
                    for (int i = 0; i < dimIdx.length; i++) {
                      dimVals[i] = dimDim.getValue(dimIdx[i]);
                    }
                    return dimVals;
                  }
                };
              }

              @Override
              public ValueDesc getColumnType(String columnName)
              {
                return resolver.resolveColumn(columnName);
              }
            };
          }
        }
    );
  }

  private static class EntryHolder
  {
    Map.Entry<IncrementalIndex.TimeAndDims, Integer> currEntry = null;

    public Map.Entry<IncrementalIndex.TimeAndDims, Integer> get()
    {
      return currEntry;
    }

    public void set(Map.Entry<IncrementalIndex.TimeAndDims, Integer> currEntry)
    {
      this.currEntry = currEntry;
    }

    public IncrementalIndex.TimeAndDims getKey()
    {
      return currEntry.getKey();
    }

    public Integer getValue()
    {
      return currEntry.getValue();
    }
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }

  public static class Temporary extends IncrementalIndexStorageAdapter
  {
    private final String dataSource;

    public Temporary(String dataSource, IncrementalIndex<?> index)
    {
      super(index);
      this.dataSource = Preconditions.checkNotNull(dataSource);
    }

    @Override
    public String getSegmentIdentifier()
    {
      // return dummy segment id to avoid exceptions in select engine
      return DataSegment.makeDataSegmentIdentifier(
          dataSource,
          getMinTime(),
          getMaxTime(),
          "temporary",
          NoneShardSpec.instance()
      );
    }
  }
}
