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

package io.druid.query.metadata;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueType;
import io.druid.granularity.QueryGranularities;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.Interval;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public class SegmentAnalyzer
{
  private static final Logger log = new Logger(SegmentAnalyzer.class);

  /**
   * This is based on the minimum size of a timestamp (POSIX seconds).  An ISO timestamp will actually be more like 24+
   */
  private static final int NUM_BYTES_IN_TIMESTAMP = 10;

  /**
   * This is based on assuming 6 units of precision, one decimal point and a single value left of the decimal
   */
  private static final int NUM_BYTES_IN_TEXT_FLOAT = 8;

  private static final int NUM_BYTES_IN_TEXT_DOUBLE = 12;

  private final EnumSet<SegmentMetadataQuery.AnalysisType> analysisTypes;

  public SegmentAnalyzer(EnumSet<SegmentMetadataQuery.AnalysisType> analysisTypes)
  {
    this.analysisTypes = analysisTypes;
  }

  public long numRows(Segment segment)
  {
    return Preconditions.checkNotNull(segment, "segment").asStorageAdapter().getNumRows();
  }

  public Map<String, ColumnAnalysis> analyze(Segment segment)
  {
    Preconditions.checkNotNull(segment, "segment");

    // index is null for incremental-index-based segments, but storageAdapter is always available
    final QueryableIndex index = segment.asQueryableIndex();
    final StorageAdapter storageAdapter = segment.asStorageAdapter();

    final Set<String> columnNames = Sets.newHashSet();
    Iterables.addAll(columnNames, storageAdapter.getAvailableDimensions());
    Iterables.addAll(columnNames, storageAdapter.getAvailableMetrics());

    Map<String, ColumnAnalysis> columns = Maps.newTreeMap();

    for (String columnName : columnNames) {
      final Column column = index == null ? null : index.getColumn(columnName);
      final ColumnCapabilities capabilities = column != null
                                              ? column.getCapabilities()
                                              : storageAdapter.getColumnCapabilities(columnName);

      final ColumnAnalysis analysis;
      final ValueType type = capabilities.getType();
      switch (type) {
        case LONG:
          analysis = analyzeNumericColumn(columnName, capabilities, storageAdapter, Longs.BYTES);
          break;
        case FLOAT:
          analysis = analyzeNumericColumn(columnName, capabilities, storageAdapter, NUM_BYTES_IN_TEXT_FLOAT);
          break;
        case DOUBLE:
          analysis = analyzeNumericColumn(columnName, capabilities, storageAdapter, NUM_BYTES_IN_TEXT_DOUBLE);
          break;
        case STRING:
          if (index != null) {
            analysis = analyzeStringColumn(columnName, capabilities, storageAdapter, column);
          } else {
            analysis = analyzeStringColumn(columnName, capabilities, storageAdapter);
          }
          break;
        case COMPLEX:
          analysis = analyzeComplexColumn(columnName, capabilities, storageAdapter, column);
          break;
        default:
          log.warn("Unknown column type[%s].", type);
          analysis = ColumnAnalysis.error(String.format("unknown_type_%s", type));
      }

      columns.put(columnName, analysis);
    }

    // Add time column too
    ColumnCapabilities timeCapabilities = storageAdapter.getColumnCapabilities(Column.TIME_COLUMN_NAME);
    if (timeCapabilities == null) {
      timeCapabilities = new ColumnCapabilitiesImpl().setType(ValueType.LONG).setHasMultipleValues(false);
    }
    columns.put(
        Column.TIME_COLUMN_NAME,
        analyzeNumericColumn(Column.TIME_COLUMN_NAME, timeCapabilities, storageAdapter, NUM_BYTES_IN_TIMESTAMP)
    );

    return columns;
  }

  public boolean analyzingSize()
  {
    return analysisTypes.contains(SegmentMetadataQuery.AnalysisType.SIZE);
  }

  public boolean analyzingSerializedSize()
  {
    return analysisTypes.contains(SegmentMetadataQuery.AnalysisType.SERIALIZED_SIZE);
  }

  public boolean analyzingCardinality()
  {
    return analysisTypes.contains(SegmentMetadataQuery.AnalysisType.CARDINALITY);
  }

  public boolean analyzingMinMax()
  {
    return analysisTypes.contains(SegmentMetadataQuery.AnalysisType.MINMAX);
  }

  private ColumnAnalysis analyzeNumericColumn(
      final String columnName,
      final ColumnCapabilities capabilities,
      final StorageAdapter storageAdapter,
      final long sizePerRow
  )
  {
    long size = 0;
    if (analyzingSize()) {
      if (capabilities.hasMultipleValues()) {
        return ColumnAnalysis.error("multi_value");
      }
      size = storageAdapter.getNumRows() * sizePerRow;
    }

    long serializedSize = 0;
    if (analyzingSerializedSize()) {
      serializedSize = storageAdapter.getSerializedSize(columnName);
    }

    return new ColumnAnalysis(
        capabilities.getType().name(),
        capabilities.hasMultipleValues(),
        size,
        serializedSize,
        null,
        null,
        null,
        null
    );
  }

  private ColumnAnalysis analyzeStringColumn(
      final String columnName,
      final ColumnCapabilities capabilities,
      final StorageAdapter storageAdapter,
      final Column column
  )
  {
    long size = 0;
    long serializedSize = 0;

    Comparable min = null;
    Comparable max = null;

    if (!capabilities.hasBitmapIndexes()) {
      return ColumnAnalysis.error("string_no_bitmap");
    }

    final BitmapIndex bitmapIndex = column.getBitmapIndex();
    final int cardinality = bitmapIndex.getCardinality();

    if (analyzingSize()) {
      for (int i = 0; i < cardinality; ++i) {
        String value = bitmapIndex.getValue(i);
        if (value != null) {
          size += StringUtils.estimatedBinaryLengthAsUTF8(value) *
                  bitmapIndex.getBitmap(bitmapIndex.getIndex(value)).size();
        }
      }
    }
    if (analyzingSerializedSize()) {
      serializedSize = storageAdapter.getSerializedSize(columnName);
    }

    if (analyzingMinMax() && cardinality > 0) {
      min = Strings.nullToEmpty(bitmapIndex.getValue(0));
      max = Strings.nullToEmpty(bitmapIndex.getValue(cardinality - 1));
    }

    return new ColumnAnalysis(
        capabilities.getType().name(),
        capabilities.hasMultipleValues(),
        size,
        serializedSize,
        analyzingCardinality() ? cardinality : 0,
        min,
        max,
        null
    );
  }

  private ColumnAnalysis analyzeStringColumn(
      final String columnName,
      final ColumnCapabilities capabilities,
      final StorageAdapter storageAdapter
  )
  {
    int cardinality = 0;
    long size = 0;
    long serializedSize = 0;

    Comparable min = null;
    Comparable max = null;

    if (analyzingCardinality()) {
      cardinality = storageAdapter.getDimensionCardinality(columnName);
    }

    if (analyzingSize()) {
      final long start = storageAdapter.getMinTime().getMillis();
      final long end = storageAdapter.getMaxTime().getMillis();

      final Sequence<Cursor> cursors =
          storageAdapter.makeCursors(
              null,
              new Interval(start, end),
              VirtualColumns.EMPTY,
              QueryGranularities.ALL,
              null,
              false
          );

      size = cursors.accumulate(
          0L,
          new Accumulator<Long, Cursor>()
          {
            @Override
            public Long accumulate(Long accumulated, Cursor cursor)
            {
              DimensionSelector selector = cursor.makeDimensionSelector(
                  new DefaultDimensionSpec(
                      columnName,
                      columnName
                  )
              );
              if (selector == null) {
                return accumulated;
              }
              long current = accumulated;
              while (!cursor.isDone()) {
                final IndexedInts vals = selector.getRow();
                for (int i = 0; i < vals.size(); ++i) {
                  final String dimVal = selector.lookupName(vals.get(i));
                  if (dimVal != null && !dimVal.isEmpty()) {
                    current += StringUtils.estimatedBinaryLengthAsUTF8(dimVal);
                  }
                }
                cursor.advance();
              }

              return current;
            }
          }
      );
    }
    if (analyzingSerializedSize()) {
      serializedSize = storageAdapter.getSerializedSize(columnName);
    }

    if (analyzingMinMax()) {
      min = storageAdapter.getMinValue(columnName);
      max = storageAdapter.getMaxValue(columnName);
    }

    return new ColumnAnalysis(
        capabilities.getType().name(),
        capabilities.hasMultipleValues(),
        size,
        serializedSize,
        cardinality,
        min,
        max,
        null
    );
  }

  private ColumnAnalysis analyzeComplexColumn(
      final String columnName,
      final ColumnCapabilities capabilities,
      final StorageAdapter storageAdapter,
      final Column column
  )
  {
    final ComplexColumn complexColumn = column != null ? column.getComplexColumn() : null;
    final boolean hasMultipleValues = capabilities != null && capabilities.hasMultipleValues();
    final String typeName = storageAdapter.getColumnTypeName(columnName);
    long size = 0;
    long serializedSize = 0;

    if (analyzingSerializedSize()) {
      serializedSize = storageAdapter.getSerializedSize(columnName);
    }
    if (analyzingSize() && complexColumn != null) {
      final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
      if (serde == null) {
        return ColumnAnalysis.error(String.format("unknown_complex_%s", typeName));
      }

      final Function<Object, Long> inputSizeFn = serde.inputSizeFn();
      if (inputSizeFn == null) {
        return new ColumnAnalysis(typeName, hasMultipleValues, 0, serializedSize, null, null, null, null);
      }

      final int length = column.getLength();
      for (int i = 0; i < length; ++i) {
        size += inputSizeFn.apply(complexColumn.getRowValue(i));
      }
    }

    return new ColumnAnalysis(
        typeName,
        hasMultipleValues,
        size,
        serializedSize,
        null,
        null,
        null,
        null
    );
  }
}
