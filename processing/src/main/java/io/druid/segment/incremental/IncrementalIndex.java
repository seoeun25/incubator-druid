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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueType;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.Metadata;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public abstract class IncrementalIndex<AggregatorType> implements Iterable<Row>, Closeable
{
  static final Logger log = new Logger(IncrementalIndex.class);

  private final AtomicLong maxIngestedEventTime = new AtomicLong(-1);

  // Used to discover ValueType based on the class of values in a row
  // Also used to convert between the duplicate ValueType enums in DimensionSchema (druid-api) and main druid.
  private static final Map<Object, ValueType> TYPE_MAP = ImmutableMap.<Object, ValueType>builder()
      .put(Long.class, ValueType.LONG)
      .put(Float.class, ValueType.FLOAT)
      .put(Double.class, ValueType.DOUBLE)
      .put(Long.TYPE, ValueType.LONG)
      .put(Float.TYPE, ValueType.FLOAT)
      .put(Double.TYPE, ValueType.DOUBLE)
      .put(String.class, ValueType.STRING)
      .build();

  private static final Function<Object, String> STRING_TRANSFORMER = new Function<Object, String>()
  {
    @Override
    public String apply(final Object o)
    {
      return o == null ? null : String.valueOf(o);
    }
  };

  private static final Function<Object, Long> LONG_TRANSFORMER = new Function<Object, Long>()
  {
    @Override
    public Long apply(final Object o)
    {
      if (o == null) {
        return null;
      }
      if (o instanceof String) {
        String s = (String) o;
        try {
          return s.isEmpty() ? null : Long.valueOf(s);
        }
        catch (NumberFormatException nfe) {
          throw new ParseException(nfe, "Unable to parse value[%s] as long in column: ", o);
        }
      }
      if (o instanceof Number) {
        return ((Number) o).longValue();
      }
      return null;
    }
  };

  private static final Function<Object, Float> FLOAT_TRANSFORMER = new Function<Object, Float>()
  {
    @Override
    public Float apply(final Object o)
    {
      if (o == null) {
        return null;
      }
      if (o instanceof String) {
        String s = (String) o;
        try {
          return s.isEmpty() ? null : Float.valueOf(s);
        }
        catch (NumberFormatException nfe) {
          throw new ParseException(nfe, "Unable to parse value[%s] as float in column: ", o);
        }
      }
      if (o instanceof Number) {
        return ((Number) o).floatValue();
      }
      return null;
    }
  };

  private static final Function<Object, Double> DOUBLE_TRANSFORMER = new Function<Object, Double>()
  {
    @Override
    public Double apply(final Object o)
    {
      if (o == null) {
        return null;
      }
      if (o instanceof String) {
        String s = (String) o;
        try {
          return s.isEmpty() ? null : Double.valueOf(s);
        }
        catch (NumberFormatException nfe) {
          throw new ParseException(nfe, "Unable to parse value[%s] as float in column: ", o);
        }
      }
      if (o instanceof Number) {
        return ((Number) o).doubleValue();
      }
      return null;
    }
  };

  private static final Map<ValueType, Function> VALUE_TRANSFORMS = ImmutableMap.<ValueType, Function>builder()
      .put(ValueType.LONG, LONG_TRANSFORMER)
      .put(ValueType.FLOAT, FLOAT_TRANSFORMER)
      .put(ValueType.DOUBLE, DOUBLE_TRANSFORMER)
      .put(ValueType.STRING, STRING_TRANSFORMER)
      .build();

  public static ColumnSelectorFactory makeColumnSelectorFactory(
      final AggregatorFactory agg,
      final Supplier<Row> in,
      final boolean deserializeComplexMetrics
  )
  {
    return new ColumnSelectorFactory.ExprSupport()
    {
      @Override
      public LongColumnSelector makeLongColumnSelector(final String columnName)
      {
        if (columnName.equals(Column.TIME_COLUMN_NAME)) {
          return new LongColumnSelector()
          {
            @Override
            public long get()
            {
              return in.get().getTimestampFromEpoch();
            }
          };
        }
        return new LongColumnSelector()
        {
          @Override
          public long get()
          {
            return in.get().getLongMetric(columnName);
          }
        };
      }

      @Override
      public FloatColumnSelector makeFloatColumnSelector(final String columnName)
      {
        return new FloatColumnSelector()
        {
          @Override
          public float get()
          {
            return in.get().getFloatMetric(columnName);
          }
        };
      }

      @Override
      public DoubleColumnSelector makeDoubleColumnSelector(final String columnName)
      {
        return new DoubleColumnSelector()
        {
          @Override
          public double get()
          {
            return in.get().getDoubleMetric(columnName);
          }
        };
      }

      @Override
      public ObjectColumnSelector makeObjectColumnSelector(final String column)
      {
        if (Column.TIME_COLUMN_NAME.equals(column)) {
          return new ObjectColumnSelector()
          {
            @Override
            public Class classOfObject()
            {
              return ValueType.LONG.classOfObject();
            }

            @Override
            public Object get()
            {
              return in.get().getTimestampFromEpoch();
            }
          };
        }

        final String typeName = agg.getInputTypeName();
        final ValueType type = ValueType.of(typeName);

        if (type != ValueType.COMPLEX || !deserializeComplexMetrics) {
          final boolean dimension = type == ValueType.COMPLEX && typeName.equals("array.string");
          return new ObjectColumnSelector()
          {
            @Override
            public Class classOfObject()
            {
              return type.classOfObject();
            }

            @Override
            public Object get()
            {
              switch (type) {
                case FLOAT:
                  return in.get().getFloatMetric(column);
                case LONG:
                  return in.get().getLongMetric(column);
                case DOUBLE:
                  return in.get().getDoubleMetric(column);
              }
              if (dimension) {
                return in.get().getDimension(column);
              }
              return in.get().getRaw(column);
            }
          };
        } else {
          final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
          if (serde == null) {
            throw new ISE("Don't know how to handle type[%s]", typeName);
          }

          final ComplexMetricExtractor extractor = serde.getExtractor();
          return new ObjectColumnSelector()
          {
            @Override
            public Class classOfObject()
            {
              return extractor.extractedClass();
            }

            @Override
            public Object get()
            {
              return extractor.extractValue(in.get(), column);
            }
          };
        }
      }

      @Override
      public ColumnCapabilities getColumnCapabilities(String columnName)
      {
        return null;
      }

      @Override
      public Iterable<String> getColumnNames()
      {
        return null;
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

        return new DimensionSelector()
        {
          @Override
          public IndexedInts getRow()
          {
            final List<String> dimensionValues = in.get().getDimension(dimension);
            final ArrayList<Integer> vals = Lists.newArrayList();
            if (dimensionValues != null) {
              for (int i = 0; i < dimensionValues.size(); ++i) {
                vals.add(i);
              }
            }

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
              public void close() throws IOException
              {

              }

              @Override
              public void fill(int index, int[] toFill)
              {
                throw new UnsupportedOperationException("fill not supported");
              }
            };
          }

          @Override
          public int getValueCardinality()
          {
            throw new UnsupportedOperationException("value cardinality is unknown in incremental index");
          }

          @Override
          public String lookupName(int id)
          {
            final String value = in.get().getDimension(dimension).get(id);
            return extractionFn == null ? value : extractionFn.apply(value);
          }

          @Override
          public int lookupId(String name)
          {
            if (extractionFn != null) {
              throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
            }
            return in.get().getDimension(dimension).indexOf(name);
          }
        };
      }
    };
  }

  private final long minTimestamp;
  private final QueryGranularity gran;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  private final AggregatorFactory[] metrics;
  private final AggregatorType[] aggs;
  private final int maxLengthForAggregators;
  private final boolean deserializeComplexMetrics;
  private final boolean reportParseExceptions;
  private final boolean sortFacts;
  private final boolean rollup;
  private final boolean fixedSchema;
  private final Metadata metadata;

  private final Map<String, MetricDesc> metricDescs;

  private final Map<String, DimensionDesc> dimensionDescs;
  private final Map<String, ColumnCapabilitiesImpl> columnCapabilities;
  private final List<DimDim> dimValues;

  // looks need a configuration
  private final Ordering<Comparable> ordering = Ordering.natural().nullsFirst();

  private final AtomicInteger numEntries = new AtomicInteger();

  // This is modified on add() in a critical section.
  private final ThreadLocal<Row> in = new ThreadLocal<>();
  private final Supplier<Row> rowSupplier = new Supplier<Row>()
  {
    @Override
    public Row get()
    {
      return in.get();
    }
  };

  private int ingestedNumRows;
  private long minTimeMillis = Long.MAX_VALUE;
  private long maxTimeMillis = Long.MIN_VALUE;

  private AtomicLong indexer = new AtomicLong();
  /**
   * Setting deserializeComplexMetrics to false is necessary for intermediate aggregation such as groupBy that
   * should not deserialize input columns using ComplexMetricSerde for aggregators that return complex metrics.
   *
   * @param incrementalIndexSchema    the schema to use for incremental index
   * @param deserializeComplexMetrics flag whether or not to call ComplexMetricExtractor.extractValue() on the input
   *                                  value for aggregators that return metrics other than float.
   * @param reportParseExceptions     flag whether or not to report ParseExceptions that occur while extracting values
   *                                  from input rows
   */
  public IncrementalIndex(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean deserializeComplexMetrics,
      final boolean reportParseExceptions,
      final boolean sortFacts
  )
  {
    this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
    this.gran = incrementalIndexSchema.getGran();
    this.metrics = incrementalIndexSchema.getMetrics();
    this.rowTransformers = new CopyOnWriteArrayList<>();
    this.deserializeComplexMetrics = deserializeComplexMetrics;
    this.reportParseExceptions = reportParseExceptions;
    this.sortFacts = sortFacts;
    this.rollup = incrementalIndexSchema.isRollup();
    this.fixedSchema = incrementalIndexSchema.isFixedSchema();

    this.metadata = new Metadata()
        .setAggregators(getCombiningAggregators(metrics))
        .setQueryGranularity(this.gran);

    this.aggs = initAggs(metrics, rowSupplier, deserializeComplexMetrics);
    this.columnCapabilities = Maps.newHashMap();

    this.metricDescs = Maps.newLinkedHashMap();
    for (AggregatorFactory metric : metrics) {
      MetricDesc metricDesc = new MetricDesc(metricDescs.size(), metric);
      metricDescs.put(metricDesc.getName(), metricDesc);
      columnCapabilities.put(metricDesc.getName(), metricDesc.getCapabilities());
    }

    DimensionsSpec dimensionsSpec = incrementalIndexSchema.getDimensionsSpec();

    this.dimensionDescs = Maps.newLinkedHashMap();
    this.dimValues = Collections.synchronizedList(Lists.<DimDim>newArrayList());

    for (DimensionSchema dimSchema : dimensionsSpec.getDimensions()) {
      ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
      ValueType type = dimSchema.getValueType();
      capabilities.setType(type);
      if (dimSchema.getTypeName().equals(DimensionSchema.SPATIAL_TYPE_NAME)) {
        capabilities.setHasSpatialIndexes(true);
      } else {
        int compareCacheEntry = dimSchema instanceof StringDimensionSchema
                               ? ((StringDimensionSchema) dimSchema).getCompareCacheEntry()
                               : -1;
        addNewDimension(dimSchema.getName(), capabilities, dimSchema.getMultiValueHandling(), compareCacheEntry);
      }
      columnCapabilities.put(dimSchema.getName(), capabilities);
    }

    // This should really be more generic
    List<SpatialDimensionSchema> spatialDimensions = dimensionsSpec.getSpatialDimensions();
    if (!spatialDimensions.isEmpty()) {
      this.rowTransformers.add(new SpatialDimensionRowTransformer(spatialDimensions));
    }
    int length = 0;
    for (int i = 0; i < metrics.length; i++) {
      if (!(metrics[i].providesEstimation())) {
        length += metrics[i].getMaxIntermediateSize();
      }
      length += 64;
    }
    this.maxLengthForAggregators = length;
  }

  @VisibleForTesting
  final DimDim newDimDim(String dimension, ValueType type, int compareCacheEntry) {
    DimDim newDimDim;
    switch (type) {
      case LONG:
        newDimDim = makeDimDim(dimension, SizeEstimator.LONG);
        break;
      case FLOAT:
        newDimDim = makeDimDim(dimension, SizeEstimator.FLOAT);
        break;
      case DOUBLE:
        newDimDim = makeDimDim(dimension, SizeEstimator.DOUBLE);
        break;
      case STRING:
        newDimDim = new NullValueConverterDimDim(
            makeDimDim(dimension, SizeEstimator.STRING),
            sortFacts ? compareCacheEntry : -1
        );
        break;
      default:
        throw new IAE("Invalid column type: " + type);
    }
    return newDimDim;
  }

  // use newDimDim() to create a DimDim, makeDimDim() provides the subclass-specific implementation
  protected abstract DimDim makeDimDim(String dimension, SizeEstimator estimator);

  protected abstract DimDim makeDimDim(String dimension, String[] dictionary, SizeEstimator estimator);

  public abstract ConcurrentMap<TimeAndDims, Integer> getFacts();

  public abstract boolean canAppendRow();

  public abstract String getOutOfRowsReason();

  protected abstract AggregatorType[] initAggs(
      AggregatorFactory[] metrics,
      Supplier<Row> rowSupplier,
      boolean deserializeComplexMetrics
  );

  // Note: This method needs to be thread safe.
  protected abstract Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      Row row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<Row> rowContainer,
      Supplier<Row> rowSupplier
  ) throws IndexSizeExceededException;

  protected abstract AggregatorType[] getAggsForRow(int rowOffset);

  protected abstract Object getAggVal(AggregatorType agg, int rowOffset, int aggPosition);

  protected abstract float getMetricFloatValue(int rowOffset, int aggOffset);

  protected abstract double getMetricDoubleValue(int rowOffset, int aggOffset);

  protected abstract long getMetricLongValue(int rowOffset, int aggOffset);

  protected abstract Object getMetricObjectValue(int rowOffset, int aggOffset);

  @Override
  public void close()
  {
    dimValues.clear();
  }

  public InputRow formatRow(InputRow row)
  {
    for (Function<InputRow, InputRow> rowTransformer : rowTransformers) {
      row = rowTransformer.apply(row);
    }

    if (row == null) {
      throw new IAE("Row is null? How can this be?!");
    }
    return row;
  }

  private ValueType getTypeFromDimVal(Object dimVal)
  {
    Object singleVal;
    if (dimVal instanceof List) {
      List dimValList = (List) dimVal;
      singleVal = dimValList.size() == 0 ? null : dimValList.get(0);
    } else {
      singleVal = dimVal;
    }

    if (singleVal == null) {
      return null;
    }

    return TYPE_MAP.get(singleVal.getClass());
  }

  private List<Comparable> getRowDimensionAsComparables(InputRow row, String dimension, ValueType type)
  {
    final Object dimVal = row.getRaw(dimension);
    final Function transformer = VALUE_TRANSFORMS.get(type);
    final List<Comparable> dimensionValues;
    try {
      if (dimVal == null) {
        dimensionValues = Collections.emptyList();
      } else if (dimVal instanceof List) {
        dimensionValues = Lists.transform((List) dimVal, transformer);
      } else {
        dimensionValues = Collections.singletonList((Comparable) transformer.apply(dimVal));
      }
    }
    catch (ParseException pe) {
      throw new ParseException(pe.getMessage() + dimension);
    }
    return dimensionValues;
  }

  public Map<String, DimensionDesc> getDimensionDescs()
  {
    return dimensionDescs;
  }

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one.
   * <p/>
   * <p/>
   * Calls to add() are thread safe.
   * <p/>
   *
   * @param row the row of data to add
   *
   * @return the number of rows in the data set after adding the InputRow
   */
  public int add(InputRow row) throws IndexSizeExceededException
  {
    return addTimeAndDims(row, toTimeAndDims(row));
  }

  private int addTimeAndDims(Row row, TimeAndDims key) throws IndexSizeExceededException
  {
    final int rv = addToFacts(
        metrics,
        deserializeComplexMetrics,
        reportParseExceptions,
        row,
        numEntries,
        key,
        in,
        rowSupplier
    );
    ingestedNumRows++;
    updateMaxIngestedTime(row.getTimestamp());
    return rv;
  }

  @VisibleForTesting
  TimeAndDims toTimeAndDims(InputRow row) throws IndexSizeExceededException
  {
    row = formatRow(row);

    final long timestampFromEpoch = row.getTimestampFromEpoch();
    if (minTimestamp != Long.MIN_VALUE && timestampFromEpoch < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, new DateTime(minTimestamp));
    }

    final List<String> rowDimensions = row.getDimensions();

    int[][] dims;
    List<int[]> overflow = null;
    List<ValueType> overflowTypes = null;
    synchronized (dimensionDescs) {
      dims = new int[dimensionDescs.size()][];
      for (String dimension : rowDimensions) {
        List<Comparable> dimensionValues;

        ColumnCapabilitiesImpl capabilities;
        final ValueType valType;
        DimensionDesc desc = dimensionDescs.get(dimension);
        if (desc != null) {
          capabilities = desc.getCapabilities();
        } else {
          capabilities = columnCapabilities.get(dimension);
          if (capabilities == null) {
            capabilities = new ColumnCapabilitiesImpl();
            // For schemaless type discovery, assume everything is a String for now, can change later.
            capabilities.setType(ValueType.STRING);
            columnCapabilities.put(dimension, capabilities);
          }
        }
        valType = capabilities.getType();
        dimensionValues = getRowDimensionAsComparables(row, dimension, valType);

        // Set column capabilities as data is coming in
        if (!capabilities.hasMultipleValues() && dimensionValues.size() > 1) {
          capabilities.setHasMultipleValues(true);
        }

        if (desc == null) {
          desc = addNewDimension(dimension, capabilities, null, -1);

          if (overflow == null) {
            overflow = Lists.newArrayList();
            overflowTypes = Lists.newArrayList();
          }
          overflow.add(getDimVals(desc, dimensionValues));
          overflowTypes.add(valType);
        } else if (desc.getIndex() > dims.length || dims[desc.getIndex()] != null) {
          /*
           * index > dims.length requires that we saw this dimension and added it to the dimensionOrder map,
           * otherwise index is null. Since dims is initialized based on the size of dimensionOrder on each call to add,
           * it must have been added to dimensionOrder during this InputRow.
           *
           * if we found an index for this dimension it means we've seen it already. If !(index > dims.length) then
           * we saw it on a previous input row (this its safe to index into dims). If we found a value in
           * the dims array for this index, it means we have seen this dimension already on this input row.
           */
          throw new ISE("Dimension[%s] occurred more than once in InputRow", dimension);
        } else {
          dims[desc.getIndex()] = getDimVals(desc, dimensionValues);
        }
      }
    }

    if (overflow != null) {
      // Merge overflow and non-overflow
      int[][] newDims = new int[dims.length + overflow.size()][];
      System.arraycopy(dims, 0, newDims, 0, dims.length);
      for (int i = 0; i < overflow.size(); ++i) {
        newDims[dims.length + i] = overflow.get(i);
      }
      dims = newDims;
    }

    return createTimeAndDims(toIndexingTime(timestampFromEpoch), dims);
  }

  private long toIndexingTime(long timestampFromEpoch)
  {
    long truncated = timestampFromEpoch;
    if (minTimestamp != Long.MIN_VALUE) {
      truncated = Math.max(gran.truncate(timestampFromEpoch), minTimestamp);
    }

    minTimeMillis = Math.min(minTimeMillis, truncated);
    maxTimeMillis = Math.max(maxTimeMillis, truncated);
    return truncated;
  }

  private TimeAndDims createTimeAndDims(long timestamp, int[][] dims)
  {
    if (rollup) {
      return new Rollup(timestamp, dims);
    }
    return new NoRollup(timestamp, dims, indexer.getAndIncrement());
  }

  public TimeAndDims createRangeTimeAndDims(long timestamp)
  {
    if (rollup) {
      return new Rollup(timestamp, new int[][]{});
    }
    return new NoRollup(timestamp, new int[][]{}, 0);
  }

  private void updateMaxIngestedTime(DateTime eventTime)
  {
    final long time = eventTime.getMillis();
    for (long current = maxIngestedEventTime.get(); time > current; current = maxIngestedEventTime.get()) {
      if (maxIngestedEventTime.compareAndSet(current, time)) {
        break;
      }
    }
  }

  // fast track for group-by query
  public void initialize(List<String> dimensions)
  {
    Preconditions.checkArgument(fixedSchema, "this is only for group-by");
    for (String dimension : dimensions) {
      addNewDimension(dimension, ColumnCapabilitiesImpl.of(ValueType.STRING), MultiValueHandling.ARRAY, -1);
    }
  }

  public void initialize(Map<String, String[]> dimensions)
  {
    Preconditions.checkArgument(fixedSchema, "this is only for group-by");
    for (Map.Entry<String, String[]> entry : dimensions.entrySet()) {
      String dimension = entry.getKey();
      DimDim values = new NullValueConverterDimDim(makeDimDim(dimension, entry.getValue(), SizeEstimator.STRING), -1);
      DimensionDesc desc = new DimensionDesc(
          dimensionDescs.size(),
          dimension,
          values,
          ColumnCapabilitiesImpl.of(ValueType.STRING),
          MultiValueHandling.ARRAY
      );
      if (dimValues.size() != desc.getIndex()) {
        throw new ISE("dimensionDescs and dimValues for [%s] is out of sync!!", dimension);
      }

      dimensionDescs.put(dimension, desc);
      dimValues.add(desc.getValues());
    }
  }

  public int add(Row row)
  {
    Preconditions.checkArgument(fixedSchema, "this is only for group-by");
    try {
      return addTimeAndDims(row, toTimeAndDims(row));
    }
    catch (IndexSizeExceededException e) {
      throw new ISE(e, e.getMessage());
    }
  }

  private TimeAndDims toTimeAndDims(Row row)
  {
    int[][] dims = new int[dimensionDescs.size()][];
    for (Map.Entry<String, DimensionDesc> entry : dimensionDescs.entrySet()) {
      DimensionDesc dimDesc = entry.getValue();
      dims[dimDesc.index] = getDimVal(dimDesc, STRING_TRANSFORMER.apply(row.getRaw(entry.getKey())));
    }
    return createTimeAndDims(toIndexingTime(row.getTimestampFromEpoch()), dims);
  }

  public boolean isEmpty()
  {
    return numEntries.get() == 0;
  }

  public int size()
  {
    return numEntries.get();
  }

  public long estimatedOccupation()
  {
    final int size = size();
    long occupation = maxLengthForAggregators * size;
    for (DimensionDesc dimensionDesc : dimensionDescs.values()) {
      occupation += dimensionDesc.getValues().estimatedSize();
      occupation += 80 * size;
    }
    return occupation;
  }

  private long getMinTimeMillis()
  {
    return minTimeMillis == Long.MAX_VALUE ? -1 : Math.max(minTimestamp, minTimeMillis);
  }

  private long getMaxTimeMillis()
  {
    return maxTimeMillis == Long.MIN_VALUE ? -1 : maxTimeMillis;
  }

  @SuppressWarnings("unchecked")
  private int[] getDimVal(final DimensionDesc dimDesc, final Comparable dimValue)
  {
    return new int[] {dimDesc.getValues().add(dimValue)};
  }

  @SuppressWarnings("unchecked")
  private int[] getDimVals(final DimensionDesc dimDesc, final List<Comparable> dimValues)
  {
    final DimDim dimLookup = dimDesc.getValues();
    if (dimValues.size() == 0) {
      // NULL VALUE
      dimLookup.add(null);
      return null;
    }

    if (dimValues.size() == 1) {
      Comparable dimVal = dimValues.get(0);
      // For Strings, return an array of dictionary-encoded IDs
      // For numerics, return the numeric values directly
      return new int[]{dimLookup.add(dimVal)};
    }

    final MultiValueHandling multiValueHandling = dimDesc.getMultiValueHandling();

    final Comparable[] dimArray = dimValues.toArray(new Comparable[dimValues.size()]);
    if (multiValueHandling != MultiValueHandling.ARRAY) {
      Arrays.sort(dimArray, ordering);
    }

    final int[] retVal = new int[dimArray.length];

    int prevId = -1;
    int pos = 0;
    for (int i = 0; i < dimArray.length; i++) {
      if (multiValueHandling != MultiValueHandling.SET) {
        retVal[pos++] = dimLookup.add(dimArray[i]);
        continue;
      }
      int index = dimLookup.add(dimArray[i]);
      if (index != prevId) {
        prevId = retVal[pos++] = index;
      }
    }

    return pos == retVal.length ? retVal : Arrays.copyOf(retVal, pos);
  }

  public AggregatorType[] getAggs()
  {
    return aggs;
  }

  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
  }

  public List<String> getDimensionNames()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.keySet());
    }
  }

  public List<DimensionDesc> getDimensions()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.values());
    }
  }

  public DimensionDesc getDimension(String dimension)
  {
    synchronized (dimensionDescs) {
      return dimensionDescs.get(dimension);
    }
  }

  public String getMetricType(String metric)
  {
    final MetricDesc metricDesc = getMetricDesc(metric);
    return metricDesc != null ? metricDesc.getType() : null;
  }

  public Class getMetricClass(String metric)
  {
    MetricDesc metricDesc = metricDescs.get(metric);
    switch (metricDesc.getCapabilities().getType()) {
      case COMPLEX:
        return ComplexMetrics.getSerdeForType(metricDesc.getType()).getObjectStrategy().getClazz();
      case FLOAT:
        return Float.TYPE;
      case DOUBLE:
        return Double.TYPE;
      case LONG:
        return Long.TYPE;
      case STRING:
        return String.class;
    }
    return null;
  }

  public Interval getInterval()
  {
    return new Interval(minTimestamp, isEmpty() ? minTimestamp : gran.next(getMaxTimeMillis()));
  }

  public DateTime getMinTime()
  {
    return isEmpty() ? null : new DateTime(getMinTimeMillis());
  }

  public DateTime getMaxTime()
  {
    return isEmpty() ? null : new DateTime(getMaxTimeMillis());
  }

  public DimDim getDimensionValues(String dimension)
  {
    DimensionDesc dimSpec = getDimension(dimension);
    return dimSpec == null ? null : dimSpec.getValues();
  }

  public List<String> getDimensionOrder()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.keySet());
    }
  }

  /*
   * Currently called to initialize IncrementalIndex dimension order during index creation
   * Index dimension ordering could be changed to initialize from DimensionsSpec after resolution of
   * https://github.com/druid-io/druid/issues/2011
   */
  public void loadDimensionIterable(Iterable<String> oldDimensionOrder)
  {
    synchronized (dimensionDescs) {
      if (!dimensionDescs.isEmpty()) {
        throw new ISE("Cannot load dimension order when existing order[%s] is not empty.", dimensionDescs.keySet());
      }
      for (String dim : oldDimensionOrder) {
        if (dimensionDescs.get(dim) == null) {
          ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
          capabilities.setType(ValueType.STRING);
          columnCapabilities.put(dim, capabilities);
          addNewDimension(dim, capabilities, null, -1);
        }
      }
    }
  }

  @GuardedBy("dimensionDescs")
  private DimensionDesc addNewDimension(
      String dim,
      ColumnCapabilitiesImpl capabilities,
      MultiValueHandling multiValueHandling,
      int compareCacheEntry
      )
  {
    DimDim values = newDimDim(dim, capabilities.getType(), compareCacheEntry);
    DimensionDesc desc = new DimensionDesc(dimensionDescs.size(), dim, values, capabilities, multiValueHandling);
    if (dimValues.size() != desc.getIndex()) {
      throw new ISE("dimensionDescs and dimValues for [%s] is out of sync!!", dim);
    }

    dimensionDescs.put(dim, desc);
    dimValues.add(desc.getValues());
    return desc;
  }

  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(metricDescs.keySet());
  }

  public List<MetricDesc> getMetrics()
  {
    return ImmutableList.copyOf(metricDescs.values());
  }

  public Integer getMetricIndex(String metricName)
  {
    MetricDesc metSpec = getMetricDesc(metricName);
    return metSpec == null ? null : metSpec.getIndex();
  }

  public MetricDesc getMetricDesc(String metricName)
  {
    return metricDescs.get(metricName);
  }

  public ColumnCapabilities getCapabilities(String column)
  {
    return columnCapabilities.get(column);
  }

  public ConcurrentNavigableMap<TimeAndDims, Integer> getSubMap(TimeAndDims start, TimeAndDims end)
  {
    if (sortFacts) {
      return ((ConcurrentNavigableMap<TimeAndDims, Integer>) getFacts()).subMap(start, end);
    } else {
      throw new UnsupportedOperationException("can't get subMap from unsorted facts data.");
    }
  }

  public Metadata getMetadata()
  {
    return metadata.setIngestedNumRow(ingestedNumRows).setRollup(rollup);
  }

  private static AggregatorFactory[] getCombiningAggregators(AggregatorFactory[] aggregators)
  {
    AggregatorFactory[] combiningAggregators = new AggregatorFactory[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      combiningAggregators[i] = aggregators[i].getCombiningFactory();
    }
    return combiningAggregators;
  }

  public Iterator<Row> iterator()
  {
    return iterable(false).iterator();
  }

  public Iterable<Row> iterable(boolean asSorted)
  {
    if (asSorted && !sortFacts) {
      throw new IllegalStateException("Cannot make sorted iterable");
    }
    return iterableWithPostAggregations(null, false);
  }

  public Iterable<Row> iterableWithPostAggregations(final List<PostAggregator> postAggs, final boolean descending)
  {
    return new Iterable<Row>()
    {
      @Override
      public Iterator<Row> iterator()
      {
        Map<TimeAndDims, Integer> facts = getFacts();
        if (descending && sortFacts) {
          facts = ((ConcurrentNavigableMap<TimeAndDims, Integer>) facts).descendingMap();
        }

        return Iterators.transform(
            facts.entrySet().iterator(),
            rowFunction(PostAggregators.decorate(postAggs, metrics))
        );
      }
    };
  }

  protected final Function<Map.Entry<TimeAndDims, Integer>, Row> rowFunction(
      final List<PostAggregator> postAggregators
  )
  {
    final List<DimensionDesc> dimensions = getDimensions();
    return new Function<Map.Entry<TimeAndDims, Integer>, Row>()
    {
      @Override
      public Row apply(final Map.Entry<TimeAndDims, Integer> input)
      {
        final TimeAndDims timeAndDims = input.getKey();
        final int rowOffset = input.getValue();

        int[][] theDims = timeAndDims.getDims(); //TODO: remove dictionary encoding for numerics later

        Map<String, Object> theVals = Maps.newLinkedHashMap();
        for (int i = 0; i < theDims.length; ++i) {
          int[] dim = theDims[i];
          DimensionDesc dimensionDesc = dimensions.get(i);
          if (dimensionDesc == null) {
            continue;
          }
          String dimensionName = dimensionDesc.getName();
          if (dim == null || dim.length == 0) {
            theVals.put(dimensionName, null);
            continue;
          }
          if (dim.length == 1) {
            theVals.put(dimensionName, dimensionDesc.getValues().getValue(dim[0]));
          } else {
            Comparable[] dimVals = new Comparable[dim.length];
            for (int j = 0; j < dimVals.length; j++) {
              dimVals[j] = dimensionDesc.getValues().getValue(dim[j]);
            }
            theVals.put(dimensionName, dimVals);
          }
        }

        AggregatorType[] aggs = getAggsForRow(rowOffset);
        for (int i = 0; i < aggs.length; ++i) {
          theVals.put(metrics[i].getName(), getAggVal(aggs[i], rowOffset, i));
        }

        for (PostAggregator postAgg : postAggregators) {
          theVals.put(postAgg.getName(), postAgg.compute(theVals));
        }

        return new MapBasedRow(timeAndDims.getTimestamp(), theVals);
      }
    };
  }

  public DateTime getMaxIngestedEventTime()
  {
    return maxIngestedEventTime.get() < 0 ? null : new DateTime(maxIngestedEventTime.get());
  }

  public int ingestedRows()
  {
    return ingestedNumRows;
  }

  public boolean isRollup()
  {
    return rollup;
  }

  public static final class DimensionDesc
  {
    private final int index;
    private final String name;
    private final DimDim values;
    private final ColumnCapabilitiesImpl capabilities;
    private final MultiValueHandling multiValueHandling;

    public DimensionDesc(int index,
                         String name,
                         DimDim values,
                         ColumnCapabilitiesImpl capabilities,
                         MultiValueHandling multiValueHandling
    )
    {
      this.index = index;
      this.name = name;
      this.values = values;
      this.capabilities = capabilities;
      this.multiValueHandling =
          multiValueHandling == null ? MultiValueHandling.ARRAY : multiValueHandling;
    }

    public int getIndex()
    {
      return index;
    }

    public String getName()
    {
      return name;
    }

    public DimDim getValues()
    {
      return values;
    }

    public ColumnCapabilitiesImpl getCapabilities()
    {
      return capabilities;
    }

    public MultiValueHandling getMultiValueHandling()
    {
      return multiValueHandling;
    }
  }

  public static final class MetricDesc
  {
    private final int index;
    private final String name;
    private final String type;
    private final ColumnCapabilitiesImpl capabilities;

    public MetricDesc(int index, AggregatorFactory factory)
    {
      this.index = index;
      this.name = factory.getName();
      this.type = factory.getTypeName();
      this.capabilities = new ColumnCapabilitiesImpl();
      if (type.equalsIgnoreCase("float")) {
        capabilities.setType(ValueType.FLOAT);
      } else if (type.equalsIgnoreCase("double")) {
        capabilities.setType(ValueType.DOUBLE);
      } else if (type.equalsIgnoreCase("long")) {
        capabilities.setType(ValueType.LONG);
      } else {
        capabilities.setType(ValueType.COMPLEX);
      }
    }

    public int getIndex()
    {
      return index;
    }

    public String getName()
    {
      return name;
    }

    public String getType()
    {
      return type;
    }

    public ColumnCapabilitiesImpl getCapabilities()
    {
      return capabilities;
    }
  }

  static interface SizeEstimator<T> {

    int estimate(T object);

    SizeEstimator<String> STRING = new SizeEstimator<String>()
    {
      @Override
      public int estimate(String object)
      {
        return Strings.isNullOrEmpty(object) ? 0 : StringUtils.estimatedBinaryLengthAsUTF8(object);
      }
    };
    SizeEstimator<Float> FLOAT = new SizeEstimator<Float>()
    {
      @Override
      public int estimate(Float object)
      {
        return object == null ? 0 : Float.BYTES;
      }
    };
    SizeEstimator<Double> DOUBLE = new SizeEstimator<Double>()
    {
      @Override
      public int estimate(Double object)
      {
        return object == null ? 0 : Double.BYTES;
      }
    };
    SizeEstimator<Long> LONG = new SizeEstimator<Long>()
    {
      @Override
      public int estimate(Long object)
      {
        return object == null ? 0 : Long.BYTES;
      }
    };
  }

  static interface DimDim<T extends Comparable<? super T>>
  {
    public int getId(T value);

    public T getValue(int id);

    public boolean contains(T value);

    public int size();

    public T getMinValue();

    public T getMaxValue();

    public int estimatedSize();

    public int add(T value);

    public int compare(int index1, int index2);

    public SortedDimLookup sort();
  }

  static interface SortedDimLookup<T extends Comparable<? super T>>
  {
    public int size();

    public int getSortedIdFromUnsortedId(int id);

    public int getUnsortedIdFromSortedId(int index);

    public T getValueFromSortedId(int index);
  }

  /**
   * implementation which converts null strings to empty strings and vice versa.
   *
   * also keeps compare result cache if user specified `compareCacheEntry` in dimension descriptor
   * with positive value n, cache uses approximately n ^ 2 / 8 bytes
   *
   * useful only for low cardinality dimensions
   *
   * with 1024, (1024 ^ 2) / 2 / 4 = 128KB will be used for cache
   * `/ 2` comes from that a.compareTo(b) = -b.compareTo(a)
   * `/ 4` comes from that each entry is stored by using 2 bits
   */
  static class NullValueConverterDimDim implements DimDim<String>
  {
    private final DimDim<String> delegate;

    private final int compareCacheEntry;
    private final byte[] cache;

    NullValueConverterDimDim(DimDim delegate, int compareCacheEntry)
    {
      this.delegate = delegate;
      this.compareCacheEntry = compareCacheEntry = Math.min(compareCacheEntry, 8192);  // occupies max 8M
      this.cache = compareCacheEntry > 0 ? new byte[(compareCacheEntry * (compareCacheEntry - 1) + 8) / 8] : null;
    }

    @Override
    public int getId(String value)
    {
      return delegate.getId(Strings.nullToEmpty(value));
    }

    @Override
    public String getValue(int id)
    {
      return Strings.emptyToNull(delegate.getValue(id));
    }

    @Override
    public boolean contains(String value)
    {
      return delegate.contains(Strings.nullToEmpty(value));
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Override
    public String getMinValue()
    {
      return Strings.nullToEmpty(delegate.getMinValue());
    }

    @Override
    public String getMaxValue()
    {
      return Strings.nullToEmpty(delegate.getMaxValue());
    }

    @Override
    public int estimatedSize()
    {
      return delegate.estimatedSize();
    }

    @Override
    public int add(String value)
    {
      return delegate.add(Strings.nullToEmpty(value));
    }

    @Override
    public SortedDimLookup sort()
    {
      return new NullValueConverterDimLookup(delegate.sort());
    }

    @Override
    public int compare(int lhsIdx, int rhsIdx)
    {
      if (cache != null && lhsIdx < compareCacheEntry && rhsIdx < compareCacheEntry) {
        final boolean leftToRight = lhsIdx > rhsIdx;
        final int[] cacheIndex = toIndex(lhsIdx, rhsIdx, leftToRight);
        final byte encoded = getCache(cacheIndex);
        if (encoded == ENCODED_NOT_EVAL) {
          final int compare = delegate.compare(lhsIdx, rhsIdx);
          setCache(cacheIndex, encode(compare, leftToRight));
          return compare;
        }
        return decode(encoded, leftToRight);
      }
      return delegate.compare(lhsIdx, rhsIdx);
    }

    // need four flags(not_yet, equal, positive, negative), allocating 2 bits for each entry
    private static final int[] MASKS = new int[]{
        0b00000011, 0b00001100, 0b00110000, 0b11000000
    };

    // compare results to be returned
    private static final int EQUALS = 0;
    private static final int POSITIVE = 1;
    private static final int NEGATIVE = -1;

    // stored format
    private static final byte ENCODED_NOT_EVAL = 0x00;
    private static final byte ENCODED_POSITIVE = 0x01;
    private static final byte ENCODED_NEGATIVE = 0x02;
    private static final byte ENCODED_EQUALS = 0x03;

    private int[] toIndex(int lhsIdx, int rhsIdx, boolean leftToRight)
    {
      final int index = leftToRight ?
                        (lhsIdx - rhsIdx - 1) + rhsIdx * (compareCacheEntry - 1) - (rhsIdx * (rhsIdx - 1) / 2) :
                        (rhsIdx - lhsIdx - 1) + lhsIdx * (compareCacheEntry - 1) - (lhsIdx * (lhsIdx - 1) / 2);

      return new int[]{index / 4, index % 4};
    }

    private byte getCache(int[] cacheIndex)
    {
      return (byte) ((cache[cacheIndex[0]] & MASKS[cacheIndex[1]]) >>> (cacheIndex[1] << 1));
    }

    private void setCache(int[] cacheIndex, byte value)
    {
      cache[cacheIndex[0]] |= (byte)(value << (cacheIndex[1] << 1));
    }

    private byte encode(int compare, boolean leftToRight)
    {
      return compare == EQUALS ? ENCODED_EQUALS : leftToRight ^ compare > 0 ? ENCODED_NEGATIVE : ENCODED_POSITIVE;
    }

    private int decode(byte encoded, boolean leftToRight)
    {
      return encoded == ENCODED_EQUALS ? EQUALS : leftToRight ^ encoded == ENCODED_POSITIVE ? NEGATIVE : POSITIVE;
    }
  }

  private static class NullValueConverterDimLookup implements SortedDimLookup<String>
  {
    private final SortedDimLookup<String> delegate;

    public NullValueConverterDimLookup(SortedDimLookup delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Override
    public int getUnsortedIdFromSortedId(int index)
    {
      return delegate.getUnsortedIdFromSortedId(index);
    }

    @Override
    public int getSortedIdFromUnsortedId(int id)
    {
      return delegate.getSortedIdFromUnsortedId(id);
    }

    @Override
    public String getValueFromSortedId(int index)
    {
      return Strings.emptyToNull(delegate.getValueFromSortedId(index));
    }
  }

  public static abstract class TimeAndDims
  {
    final long timestamp;
    final int[][] dims;

    TimeAndDims(
        long timestamp,
        int[][] dims
    )
    {
      this.timestamp = timestamp;
      this.dims = dims;
    }

    long getTimestamp()
    {
      return timestamp;
    }

    int[][] getDims()
    {
      return dims;
    }
  }

  public static final class Rollup extends TimeAndDims {

    Rollup(long timestamp, int[][] dims)
    {
      super(timestamp, dims);
    }

    @Override
    public String toString()
    {
      return "Rollup{" +
             "timestamp=" + new DateTime(timestamp) +
             ", dims=" + Lists.transform(
          Arrays.asList(dims), new Function<int[], Object>()
          {
            @Override
            public Object apply(@Nullable int[] input)
            {
              if (input == null || input.length == 0) {
                return Arrays.asList("null");
              }
              return Arrays.asList(input);
            }
          }
      ) + '}';
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

      TimeAndDims that = (TimeAndDims) o;

      if (timestamp != that.timestamp) {
        return false;
      }
      if (dims.length != that.dims.length) {
        return false;
      }
      for (int i = 0; i < dims.length; i++) {
        if (!Arrays.equals(dims[i], that.dims[i])) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode()
    {
      int hash = (int) timestamp;
      for (int[] dim : dims) {
        hash = 31 * hash + Arrays.hashCode(dim);
      }
      return hash;
    }
  }

  public static final class NoRollup extends TimeAndDims {

    private final long indexer;

    NoRollup(long timestamp, int[][] dims, long indexer)
    {
      super(timestamp, dims);
      this.indexer = indexer;
    }

    @Override
    public String toString()
    {
      return "NoRollup{" +
             "timestamp=" + new DateTime(timestamp) +
             ", indexer=" + indexer + "}";
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

      NoRollup that = (NoRollup) o;

      if (timestamp != that.timestamp) {
        return false;
      }
      if (indexer != that.indexer) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode()
    {
      return Longs.hashCode(timestamp) * 31 + Longs.hashCode(indexer);
    }
  }

  protected final Comparator<TimeAndDims> dimsComparator()
  {
    if (fixedSchema && !dimensionDescs.isEmpty() && isAllReadOnly()) {
      final int length = dimensionDescs.size();
      return new Comparator<TimeAndDims>()
      {
        @Override
        public int compare(TimeAndDims o1, TimeAndDims o2)
        {
          int compare = Longs.compare(o1.timestamp, o2.timestamp);
          if (compare != 0) {
            return compare;
          }
          final int[][] lhsIdxs = o1.dims;
          final int[][] rhsIdxs = o2.dims;
          for (int i = 0; i < length; i++) {
            compare = Ints.compare(lhsIdxs[i][0], rhsIdxs[i][0]);
            if (compare != 0) {
              return compare;
            }
          }
          return 0;
        }
      };
    }
    if (rollup) {
      return new TimeAndDimsComp(dimValues);
    }
    return new Comparator<TimeAndDims>()
    {
      @Override
      public int compare(TimeAndDims o1, TimeAndDims o2)
      {
        NoRollup nr1 = (NoRollup)o1;
        NoRollup nr2 = (NoRollup)o2;
        int compare = Longs.compare(nr1.timestamp, nr2.timestamp);
        if (compare == 0) {
          compare = Longs.compare(nr1.indexer, nr2.indexer);
        }
        return compare;
      }
    };
  }

  private boolean isAllReadOnly()
  {
    for (DimensionDesc dimensionDesc : dimensionDescs.values()) {
      if (!(dimensionDesc.getValues() instanceof ReadOnlyDimDim)) {
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  static final class TimeAndDimsComp implements Comparator<TimeAndDims>
  {
    private final List<DimDim> dimValues;

    public TimeAndDimsComp(List<DimDim> dimValues)
    {
      this.dimValues = dimValues;
    }

    @Override
    public int compare(TimeAndDims lhs, TimeAndDims rhs)
    {
      int retVal = Longs.compare(lhs.timestamp, rhs.timestamp);
      int numComparisons = Math.min(lhs.dims.length, rhs.dims.length);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final int[] lhsIdxs = lhs.dims[index];
        final int[] rhsIdxs = rhs.dims[index];

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        retVal = Ints.compare(lhsIdxs.length, rhsIdxs.length);

        int valsIndex = 0;
        while (retVal == 0 && valsIndex < lhsIdxs.length) {
          if (lhsIdxs[valsIndex] != rhsIdxs[valsIndex]) {
            final DimDim dimLookup = dimValues.get(index);
            if (dimLookup instanceof ReadOnlyDimDim) {
              retVal = Ints.compare(lhsIdxs[valsIndex], rhsIdxs[valsIndex]);
            } else {
              retVal = dimLookup.compare(lhsIdxs[valsIndex], rhsIdxs[valsIndex]);
            }
          }
          ++valsIndex;
        }
        ++index;
      }

      if (retVal == 0) {
        return Ints.compare(lhs.dims.length, rhs.dims.length);
      }

      return retVal;
    }
  }

  static final class ReadOnlyDimDim implements DimDim<String>
  {
    private final Map<String, Integer> valueToId;
    private final String[] idToValue;

    public ReadOnlyDimDim(String[] dictionary)
    {
      idToValue = dictionary;
      valueToId = Maps.newHashMapWithExpectedSize(dictionary.length);
      for (int i = 0; i < dictionary.length; i++) {
        valueToId.put(dictionary[i], i);
      }
    }

    public int getId(String value)
    {
      return valueToId.get(value);
    }

    public String getValue(int id)
    {
      return idToValue[id];
    }

    public boolean contains(String value)
    {
      return valueToId.containsKey(value);
    }

    public int size()
    {
      return valueToId.size();
    }

    public int add(String value)
    {
      Integer prev = valueToId.get(value);
      if (prev != null) {
        return prev;
      }
      throw new IllegalStateException("not existing value " + value);
    }

    @Override
    public String getMinValue()
    {
      throw new UnsupportedOperationException("getMinValue");
    }

    @Override
    public String getMaxValue()
    {
      throw new UnsupportedOperationException("getMaxValue");
    }

    @Override
    public int estimatedSize()
    {
      throw new UnsupportedOperationException("estimatedSize");
    }

    @Override
    public final int compare(int lhsIdx, int rhsIdx)
    {
      return Ints.compare(lhsIdx, rhsIdx);
    }

    public SortedDimLookup<String> sort()
    {
      throw new UnsupportedOperationException("sort");
    }
  }
}
