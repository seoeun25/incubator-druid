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

package io.druid.segment;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 */
public class ColumnSelectorFactories
{
  public static class NotSupports extends ColumnSelectorFactory.ExprUnSupport
  {
    @Override
    public Iterable<String> getColumnNames()
    {
      throw new UnsupportedOperationException("getColumnNames");
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("makeDimensionSelector");
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeFloatColumnSelector");
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeDoubleColumnSelector");
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeLongColumnSelector");
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeObjectColumnSelector");
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      throw new UnsupportedOperationException("getColumnType");
    }
  }

  public static class Delegated implements ColumnSelectorFactory
  {
    protected final ColumnSelectorFactory delegate;

    public Delegated(ColumnSelectorFactory delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return delegate.makeFloatColumnSelector(columnName);
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return delegate.makeDoubleColumnSelector(columnName);
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return delegate.makeLongColumnSelector(columnName);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return delegate.makeObjectColumnSelector(columnName);
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      return delegate.makeMathExpressionSelector(expression);
    }

    @Override
    public ValueMatcher makePredicateMatcher(DimFilter filter)
    {
      return delegate.makePredicateMatcher(filter);
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      return delegate.getColumnType(columnName);
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return delegate.getColumnNames();
    }
  }

  public static abstract class ArrayIndexed extends ColumnSelectorFactory.ExprUnSupport
  {
    protected final ObjectColumnSelector selector;
    protected final ValueDesc elementType;

    protected ArrayIndexed(ObjectColumnSelector selector, ValueDesc elementType)
    {
      this.selector = selector;
      this.elementType = elementType;
    }

    public ObjectColumnSelector getSelector()
    {
      return selector;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("makeDimensionSelector");
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      throw new UnsupportedOperationException("getColumnNames");
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      throw new UnsupportedOperationException("getColumnType");
    }

    protected abstract Object getObject();

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          Object value = getObject();
          if (value == null) {
            return 0.0f;
          }
          if (value instanceof Number) {
            return ((Number) value).floatValue();
          }
          return Float.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public double get()
        {
          Object value = getObject();
          if (value == null) {
            return 0.0d;
          }
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          }
          return Double.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          Object value = getObject();
          if (value == null) {
            return 0L;
          }
          if (value instanceof Number) {
            return ((Number) value).longValue();
          }
          return Long.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public ValueDesc type()
        {
          return elementType;
        }

        @Override
        public Object get()
        {
          return getObject();
        }
      };
    }
  }

  public static final class FixedArrayIndexed extends ArrayIndexed
  {
    private final int index;

    public FixedArrayIndexed(int index, ObjectColumnSelector selector, ValueDesc elementType)
    {
      super(selector, elementType);
      this.index = index;
    }

    protected final Object getObject()
    {
      List value = (List) selector.get();
      return value == null ? null : value.get(index);
    }
  }

  public static final class VariableArrayIndexed extends ArrayIndexed
  {
    private int index = -1;

    public VariableArrayIndexed(ObjectColumnSelector selector, ValueDesc elementType)
    {
      super(selector, elementType);
    }

    public void setIndex(int index)
    {
      this.index = index;
    }

    protected final Object getObject()
    {
      List value = (List) selector.get();
      return value == null ? null : value.get(index);
    }
  }

  public static final class FromRow extends AbstractFromRow
  {
    private final TypeResolver resolver;

    public FromRow(
        Supplier<Row> in,
        TypeResolver resolver,
        boolean deserializeComplexMetrics
    )
    {
      super(in, deserializeComplexMetrics);
      this.resolver = resolver;
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      return resolver.resolveColumn(columnName);
    }
  }

  // it's stupid
  public static final class FromInputRow extends AbstractFromRow
  {
    private final Set<String> required;
    private final ValueDesc valueDesc;

    public FromInputRow(
        Supplier<Row> in,
        AggregatorFactory factory,
        boolean deserializeComplexMetrics
    )
    {
      super(in, deserializeComplexMetrics);
      this.valueDesc = ValueDesc.of(factory.getInputTypeName());
      this.required = Sets.newHashSet(factory.requiredFields());
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      return required.contains(columnName) ? valueDesc : null;
    }
  }

  // Caches references to selector objects for each column instead of creating a new object each time in order to save heap space.
  // In general the selectorFactory need not to thread-safe.
  // here its made thread safe to support the special case of groupBy where the multiple threads can add concurrently to the IncrementalIndex.
  public static class Caching extends Delegated
  {
    private final Map<String, LongColumnSelector> longColumnSelectorMap = Maps.newConcurrentMap();
    private final Map<String, FloatColumnSelector> floatColumnSelectorMap = Maps.newConcurrentMap();
    private final Map<String, DoubleColumnSelector> doubleColumnSelectorMap = Maps.newConcurrentMap();
    private final Map<String, ObjectColumnSelector> objectColumnSelectorMap = Maps.newConcurrentMap();
    private final Map<String, ExprEvalColumnSelector> exprColumnSelectorMap = Maps.newConcurrentMap();

    private final Function<String, LongColumnSelector> LONG = new Function<String, LongColumnSelector>()
    {
      @Override
      public LongColumnSelector apply(String columnName)
      {
        return delegate.makeLongColumnSelector(columnName);
      }
    };
    private final Function<String, FloatColumnSelector> FLOAT = new Function<String, FloatColumnSelector>()
    {
      @Override
      public FloatColumnSelector apply(String columnName)
      {
        return delegate.makeFloatColumnSelector(columnName);
      }
    };
    private final Function<String, DoubleColumnSelector> DOUBLE = new Function<String, DoubleColumnSelector>()
    {
      @Override
      public DoubleColumnSelector apply(String columnName)
      {
        return delegate.makeDoubleColumnSelector(columnName);
      }
    };
    private final Function<String, ObjectColumnSelector> OBJECT = new Function<String, ObjectColumnSelector>()
    {
      @Override
      public ObjectColumnSelector apply(String columnName)
      {
        return delegate.makeObjectColumnSelector(columnName);
      }
    };
    private final Function<String, ExprEvalColumnSelector> EXPR = new Function<String, ExprEvalColumnSelector>()
    {
      @Override
      public ExprEvalColumnSelector apply(String expression)
      {
        return delegate.makeMathExpressionSelector(expression);
      }
    };

    public Caching(ColumnSelectorFactory delegate)
    {
      super(delegate);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return floatColumnSelectorMap.computeIfAbsent(columnName, FLOAT);
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return doubleColumnSelectorMap.computeIfAbsent(columnName, DOUBLE);
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return longColumnSelectorMap.computeIfAbsent(columnName, LONG);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return objectColumnSelectorMap.computeIfAbsent(columnName, OBJECT);
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      return exprColumnSelectorMap.computeIfAbsent(expression, EXPR);
    }

    public ColumnSelectorFactory asReadOnly(AggregatorFactory... factories)
    {
      for (AggregatorFactory factory : factories) {
        factory.factorize(this);
      }
      final Map<String, LongColumnSelector> longColumnSelectorMap = ImmutableMap.copyOf(this.longColumnSelectorMap);
      final Map<String, FloatColumnSelector> floatColumnSelectorMap = ImmutableMap.copyOf(this.floatColumnSelectorMap);
      final Map<String, DoubleColumnSelector> doubleColumnSelectorMap = ImmutableMap.copyOf(this.doubleColumnSelectorMap);
      final Map<String, ObjectColumnSelector> objectColumnSelectorMap = ImmutableMap.copyOf(this.objectColumnSelectorMap);
      final Map<String, ExprEvalColumnSelector> exprColumnSelectorMap = ImmutableMap.copyOf(this.exprColumnSelectorMap);

      return new Delegated(delegate)
      {
        @Override
        public FloatColumnSelector makeFloatColumnSelector(String columnName)
        {
          return floatColumnSelectorMap.get(columnName);
        }

        @Override
        public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
        {
          return doubleColumnSelectorMap.get(columnName);
        }

        @Override
        public LongColumnSelector makeLongColumnSelector(String columnName)
        {
          return longColumnSelectorMap.get(columnName);
        }

        @Override
        public ObjectColumnSelector makeObjectColumnSelector(String columnName)
        {
          return objectColumnSelectorMap.get(columnName);
        }

        @Override
        public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
        {
          return exprColumnSelectorMap.get(expression);
        }
      };
    }
  }

  public static abstract class AbstractFromRow extends ColumnSelectorFactory.ExprSupport
  {
    private final Supplier<Row> in;
    private final boolean deserializeComplexMetrics;

    public AbstractFromRow(
        final Supplier<Row> in,
        final boolean deserializeComplexMetrics
    )
    {
      this.in = in;
      this.deserializeComplexMetrics = deserializeComplexMetrics;
    }

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
          public ValueDesc type()
          {
            return ValueDesc.LONG;
          }

          @Override
          public Object get()
          {
            return in.get().getTimestampFromEpoch();
          }
        };
      }

      final ValueDesc type = getColumnType(column);
      if (type == null || type.equals(ValueDesc.UNKNOWN)) {
        return new ObjectColumnSelector()
        {
          @Override
          public Object get()
          {
            return in.get().getRaw(column);
          }

          @Override
          public ValueDesc type()
          {
            return ValueDesc.UNKNOWN;
          }
        };
      }

      final boolean dimension = ValueDesc.isDimension(type);

      if (dimension || ValueDesc.isPrimitive(type) || !deserializeComplexMetrics) {
        return new ObjectColumnSelector()
        {
          @Override
          public ValueDesc type()
          {
            return type;
          }

          @Override
          public Object get()
          {
            if (dimension) {
              return in.get().getDimension(column);
            }
            switch (type.type()) {
              case FLOAT:
                return in.get().getFloatMetric(column);
              case LONG:
                return in.get().getLongMetric(column);
              case DOUBLE:
                return in.get().getDoubleMetric(column);
            }
            return in.get().getRaw(column);
          }
        };
      } else {
        final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(type.typeName());
        if (serde == null) {
          throw new ISE("Don't know how to handle type[%s]", type.typeName());
        }

        final ComplexMetricExtractor extractor = serde.getExtractor();
        return new ObjectColumnSelector()
        {
          @Override
          public ValueDesc type()
          {
            return type;
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
    public Iterable<String> getColumnNames()
    {
      return in.get().getColumns();
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
    }

    private DimensionSelector makeDimensionSelectorUndecorated(DimensionSpec dimensionSpec)
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
  }
}