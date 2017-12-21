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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.BitmapType;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.data.IndexedID;
import io.druid.segment.filter.Filters;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class RowResolver implements TypeResolver
{
  public static RowResolver of(Segment segment, VirtualColumns virtualColumns)
  {
    QueryableIndex queryable = segment.asQueryableIndex(false);
    if (queryable != null) {
      return new RowResolver(queryable, virtualColumns);
    }
    return new RowResolver(segment.asStorageAdapter(false), virtualColumns);
  }

  public static RowResolver of(QueryableIndex index, VirtualColumns virtualColumns)
  {
    return index == null ? null : new RowResolver(index, virtualColumns);
  }

  public static RowResolver of(GroupByQuery groupBy)
  {
    Preconditions.checkArgument(!(groupBy.getDataSource() instanceof ViewDataSource), "fix this");
    return new RowResolver(
        Lists.transform(groupBy.getDimensions(), DimensionSpecs.OUTPUT_NAME),
        groupBy.getAggregatorSpecs(),
        VirtualColumns.valueOf(groupBy.getVirtualColumns())
    );
  }

  public static Class<?> toClass(String typeName)
  {
    ValueDesc valueDesc = ValueDesc.of(typeName);
    switch (valueDesc.type()) {
      case STRING:
        return String.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case LONG:
        return Long.class;
    }
    switch (typeName.toLowerCase()) {
      case ValueDesc.MAP_TYPE:
        return Map.class;
      case ValueDesc.LIST_TYPE:
        return List.class;
      case ValueDesc.DATETIME_TYPE:
        return DateTime.class;
      case ValueDesc.UNKNOWN_TYPE:
        return Object.class;
    }

    if (typeName.startsWith(ValueDesc.INDEXED_ID_PREFIX)) {
      return IndexedID.class;
    }

    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
    if (serde != null) {
      return serde.getObjectStrategy().getClazz();
    }
    return Object.class;
  }

  public static ValueDesc toValueType(Object obj)
  {
    return obj == null ? ValueDesc.UNKNOWN : toValueType(obj.getClass(), obj);
  }

  private static ValueDesc toValueType(Class clazz, Object object)
  {
    if (clazz == Object.class) {
      return ValueDesc.UNKNOWN;
    } else if (clazz == String.class) {
      return ValueDesc.STRING;
    } else if (clazz == Float.class || clazz == Float.TYPE) {
      return ValueDesc.FLOAT;
    } else if (clazz == Double.class || clazz == Double.TYPE) {
      return ValueDesc.DOUBLE;
    } else if (clazz == Long.class || clazz == Long.TYPE) {
      return ValueDesc.LONG;
    } else if (Map.class.isAssignableFrom(clazz)) {
      return ValueDesc.MAP;
    } else if (List.class.isAssignableFrom(clazz)) {
      return ValueDesc.LIST;
    } else if (IndexedID.class.isAssignableFrom(clazz)) {
      IndexedID lookup = (IndexedID)object;
      return lookup == null ? ValueDesc.INDEXED_ID : ValueDesc.ofIndexedId(lookup.elementType());
    } else if (clazz.isArray()) {
      return ValueDesc.ofArray(toValueType(clazz.getComponentType(), null));
    }
    // cannot make multi-valued type from class

    String typeName = ComplexMetrics.getTypeNameForClass(clazz);
    if (typeName != null) {
      return ValueDesc.of(typeName);
    }
    return ValueDesc.UNKNOWN;
  }

  private final Map<String, ColumnCapabilities> columnCapabilities = Maps.newHashMap();
  private final Map<String, ValueDesc> columnTypes = Maps.newHashMap();
  private final Map<String, Pair<VirtualColumn, ValueDesc>> virtualColumnTypes = Maps.newHashMap();
  private final VirtualColumns virtualColumns;

  public RowResolver(StorageAdapter adapter, VirtualColumns virtualColumns)
  {
    for (String dimension : adapter.getAvailableDimensions()) {
      columnTypes.put(dimension, adapter.getColumnType(dimension));
      columnCapabilities.put(dimension, adapter.getColumnCapabilities(dimension));
    }
    for (String metric : adapter.getAvailableMetrics()) {
      columnTypes.put(metric, adapter.getColumnType(metric));
      columnCapabilities.put(metric, adapter.getColumnCapabilities(metric));
    }
    columnTypes.put(Column.TIME_COLUMN_NAME, ValueDesc.of(ValueType.LONG));
    columnCapabilities.put(Column.TIME_COLUMN_NAME, ColumnCapabilitiesImpl.of(ValueType.LONG));
    this.virtualColumns = virtualColumns;
  }

  private RowResolver(List<String> dimensions, List<AggregatorFactory> metrics, VirtualColumns virtualColumns)
  {
    for (String dimension : dimensions) {
      columnTypes.put(dimension, ValueDesc.ofDimension(ValueType.STRING));
    }
    for (AggregatorFactory metric : metrics) {
      columnTypes.put(metric.getName(), ValueDesc.of(metric.getTypeName()));
    }
    columnTypes.put(Column.TIME_COLUMN_NAME, ValueDesc.of(ValueType.LONG));
    this.virtualColumns = virtualColumns;
  }

  private RowResolver(QueryableIndex index, VirtualColumns virtualColumns)
  {
    for (String dimension : index.getColumnNames()) {
      Column column = index.getColumn(dimension);
      columnTypes.put(dimension, index.getColumnType(dimension));
      columnCapabilities.put(dimension, column.getCapabilities());
    }
    columnTypes.put(Column.TIME_COLUMN_NAME, index.getColumnType(Column.TIME_COLUMN_NAME));
    columnCapabilities.put(Column.TIME_COLUMN_NAME, index.getColumn(Column.TIME_COLUMN_NAME).getCapabilities());
    this.virtualColumns = virtualColumns;
  }

  @VisibleForTesting
  public RowResolver(Map<String, ValueDesc> columnTypes, VirtualColumns virtualColumns)
  {
    this.columnTypes.putAll(columnTypes);
    this.virtualColumns = virtualColumns;
  }

  @Override
  public ValueDesc resolveColumn(String column)
  {
    return resolveColumn(column, null);
  }

  public ValueDesc resolveColumn(String column, ValueDesc defaultType)
  {
    ValueDesc columnType = columnTypes.get(column);
    if (columnType != null) {
      return columnType;
    }
    Pair<VirtualColumn, ValueDesc> vcResolved = virtualColumnTypes.get(column);
    if (vcResolved != null) {
      return vcResolved.rhs;
    }
    VirtualColumn vc = virtualColumns.getVirtualColumn(column);
    if (vc != null) {
      ValueDesc valueType = vc.resolveType(column, this);
      if (valueType != null) {
        virtualColumnTypes.put(column, Pair.of(vc, valueType));
        return valueType;
      }
    }
    return defaultType;
  }

  public VirtualColumn resolveVC(String column)
  {
    if (resolveColumn(column) == null) {
      return null;
    }
    Pair<VirtualColumn, ValueDesc> resolved = virtualColumnTypes.get(column);
    if (resolved != null && resolved.lhs != null) {
      return resolved.lhs;
    }
    return null;
  }

  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return columnCapabilities.get(column);
  }

  public Iterable<String> getAllColumnNames()
  {
    Set<String> names = Sets.newLinkedHashSet(virtualColumns.getVirtualColumnNames());
    names.addAll(columnTypes.keySet()); // override
    return names;
  }

  public boolean supportsBitmap(DimFilter filter, EnumSet<BitmapType> using)
  {
    Set<String> dependents = Filters.getDependents(filter);
    return dependents.size() == 1 && supportsBitmap(Iterables.getOnlyElement(dependents), using);
  }

  private boolean supportsBitmap(String column, EnumSet<BitmapType> using)
  {
    ColumnCapabilities capabilities = columnCapabilities.get(column);
    if (capabilities == null) {
      return using.contains(BitmapType.DIMENSIONAL) && ValueDesc.isDimension(resolveColumn(column));
    }
    if (using.contains(BitmapType.DIMENSIONAL) && capabilities.hasBitmapIndexes()) {
      return true;
    }
    if (using.contains(BitmapType.LUCENE_INDEX) && capabilities.hasLuceneIndex()) {
      return true;
    }
    if (using.contains(BitmapType.METRIC_HISTOGRAM) && capabilities.hasMetricBitmap()) {
      return true;
    }
    return false;
  }

  public boolean supportsExactBitmap(Iterable<String> columns, DimFilter filter)
  {
    for (String column : columns) {
      if (!supportsBitmap(column, BitmapType.EXACT)) {
        return false;
      }
    }
    return filter == null || supportsBitmap(filter, BitmapType.EXACT);
  }
}