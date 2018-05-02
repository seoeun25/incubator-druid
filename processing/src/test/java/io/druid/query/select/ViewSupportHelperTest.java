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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import io.druid.cache.Cache;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.Druids;
import io.druid.query.QueryContextKeys;
import io.druid.query.RowResolver;
import io.druid.query.TableDataSource;
import io.druid.query.ViewDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.Metadata;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.StorageAdapterSegment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.ListIndexed;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ViewSupportHelperTest
{
  public static class TestStorageAdapter implements StorageAdapter
  {
    private final List<String> dimensions;
    private final List<String> metrics = Lists.newArrayList();
    private final Map<String, ValueDesc> typesMap = Maps.newHashMap();
    private final Metadata metadata = new Metadata();

    public TestStorageAdapter(List<String> dimensions, List<AggregatorFactory> aggregators)
    {
      this.dimensions = dimensions;
      for (String dimension : dimensions) {
        typesMap.put(dimension, ValueDesc.ofDimension(ValueType.STRING));
      }
      for (AggregatorFactory factory : aggregators) {
        typesMap.put(factory.getName(), ValueDesc.of(factory.getTypeName()));
        metrics.add(factory.getName());
      }
      metadata.setAggregators(aggregators.toArray(new AggregatorFactory[aggregators.size()]));
    }

    @Override
    public String getSegmentIdentifier()
    {
      return null;
    }

    @Override
    public Interval getInterval()
    {
      return null;
    }

    @Override
    public Indexed<String> getAvailableDimensions()
    {
      return new ListIndexed<String>(dimensions, String.class);
    }

    @Override
    public Iterable<String> getAvailableMetrics()
    {
      return metrics;
    }

    @Override
    public int getDimensionCardinality(String column)
    {
      return -1;
    }

    @Override
    public DateTime getMinTime()
    {
      return null;
    }

    @Override
    public DateTime getMaxTime()
    {
      return null;
    }

    @Override
    public Comparable getMinValue(String column)
    {
      return null;
    }

    @Override
    public Comparable getMaxValue(String column)
    {
      return null;
    }

    @Override
    public Capabilities getCapabilities()
    {
      return null;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      ValueDesc desc = typesMap.get(column);
      return desc == null ? null :
             ValueDesc.isDimension(desc) ? ColumnCapabilitiesImpl.of(ValueDesc.typeOfDimension(desc)) :
             ColumnCapabilitiesImpl.of(desc.type());
    }

    @Override
    public ValueDesc getColumnType(String column)
    {
      return typesMap.get(column);
    }

    @Override
    public long getSerializedSize(String column)
    {
      return 0;
    }

    @Override
    public float getAverageSize(String column)
    {
      return 0;
    }

    @Override
    public int getNumRows()
    {
      return 0;
    }

    @Override
    public DateTime getMaxIngestedEventTime()
    {
      return null;
    }

    @Override
    public Metadata getMetadata()
    {
      return metadata;
    }

    @Override
    public Sequence<Cursor> makeCursors(
        DimFilter filter,
        Interval interval,
        RowResolver resolver,
        Granularity gran,
        Cache cache,
        boolean descending
    )
    {
      return null;
    }
  }

  private final AggregatorFactory met1 = new DoubleMinAggregatorFactory("met1", "");
  private final AggregatorFactory met2 = new LongSumAggregatorFactory("met2", "");
  private final AggregatorFactory met3 = new LongMinAggregatorFactory("met3", "");

  private final StorageAdapter adapter = new TestStorageAdapter(
      Arrays.asList("dim1", "dim2"),
      Arrays.asList(met1, met2)
  );
  private final VirtualColumn vc1 = new ExprVirtualColumn("met1 + met2", "vc1");
  private final VirtualColumn vc2 = new ExprVirtualColumn("met1 + met3", "vc2");
  private final VirtualColumn vc3 = new ExprVirtualColumn("met3 + met4", "vc3");

  @Test
  public void testRowResolverMerge()
  {
    Segment segment1 = new StorageAdapterSegment("id1", adapter);
    Segment segment2 = new StorageAdapterSegment(
        "id2", new TestStorageAdapter(Arrays.asList("dim1", "dim3"), Arrays.asList(met1, met2, met3))
    );
    RowResolver merged = RowResolver.of(Arrays.asList(segment1, segment2), VirtualColumns.empty());
    Assert.assertEquals(Arrays.asList("dim1", "dim2", "dim3"), merged.getDimensionNames());
    Assert.assertEquals(Arrays.asList("met1", "met2", "met3"), merged.getMetricNames());
  }

  @Test
  public void testBasicOverrideGroupBy()
  {
    ViewDataSource view = new ViewDataSource(
        "test",
        Arrays.asList("dim1", "met1", "met2", "vc1", "vc2", "vc3"),
        Arrays.<VirtualColumn>asList(vc1, vc2),
        null,
        false
    );

    // allDimensionsForEmpty = false
    GroupByQuery base = new GroupByQuery.Builder()
        .setDataSource(view)
        .setGranularity(Granularities.ALL)
        .build();

    RowResolver resolver = RowResolver.of(adapter, VirtualColumns.valueOf(view.getVirtualColumns()));
    GroupByQuery x = (GroupByQuery) ViewSupportHelper.rewrite(base, resolver);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec(), x.getDimensions());
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(), x.getAggregatorSpecs());

    // allDimensionsForEmpty = true
    GroupByQuery q0 = base.withOverriddenContext(
        ImmutableMap.<String, Object>of(QueryContextKeys.ALL_DIMENSIONS_FOR_EMPTY, true)
    );
    RowResolver r0 = RowResolver.of(adapter, VirtualColumns.valueOf(q0.getVirtualColumns()));

    x = (GroupByQuery) ViewSupportHelper.rewrite(q0, r0);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim1"), x.getDimensions());  // dimension from segment
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(), x.getAggregatorSpecs());

    // override dimension
    GroupByQuery q1 = q0.withDimensionSpecs(DefaultDimensionSpec.toSpec("dim2"));
    RowResolver r1 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q1));

    x = (GroupByQuery) ViewSupportHelper.rewrite(q1, r1);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim2"), x.getDimensions());  // do not modify
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(), x.getAggregatorSpecs());

    // allMetricsForEmpty = true
    GroupByQuery q2 = base.withOverriddenContext(
        ImmutableMap.<String, Object>of(QueryContextKeys.ALL_METRICS_FOR_EMPTY, true)
    );
    RowResolver r2 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q2));

    x = (GroupByQuery) ViewSupportHelper.rewrite(q2, r2);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec(), x.getDimensions());  // dimension from segment
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(met1, met2), x.getAggregatorSpecs());

    // override metric
    GroupByQuery q3 = q2.withAggregatorSpecs(Arrays.<AggregatorFactory>asList(met3));
    RowResolver r3 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q3));

    x = (GroupByQuery) ViewSupportHelper.rewrite(q3, r3);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec(), x.getDimensions());  // dimension from segment
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(met3), x.getAggregatorSpecs());

    // override vc
    GroupByQuery q4 = base.withVirtualColumns(Arrays.<VirtualColumn>asList(vc3));
    RowResolver r4 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q4));

    x = (GroupByQuery) ViewSupportHelper.rewrite(q4, r4);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2, vc3), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec(), x.getDimensions());
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(), x.getAggregatorSpecs());
  }

  @Test
  public void testBasicOverrideSelect()
  {
    ViewDataSource view = new ViewDataSource(
        "test",
        Arrays.asList("dim1", "met1", "vc1", "vc2", "vc3"),
        Arrays.<VirtualColumn>asList(vc1, vc2),
        null,
        false
    );

    SelectQuery base = new Druids.SelectQueryBuilder()
        .dataSource(view)
        .granularity(Granularities.ALL)
        .build();
    RowResolver resolver = RowResolver.of(adapter, BaseQuery.getVirtualColumns(base));

    SelectQuery x = (SelectQuery) ViewSupportHelper.rewrite(base, resolver);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim1"), x.getDimensions());
    Assert.assertEquals(Arrays.asList("met1", "vc1", "vc2"), x.getMetrics());

    // override dimension
    SelectQuery q1 = base.withDimensionSpecs(DefaultDimensionSpec.toSpec("dim2"));
    RowResolver r1 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q1));

    x = (SelectQuery) ViewSupportHelper.rewrite(q1, r1);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim2"), x.getDimensions());
    Assert.assertEquals(Arrays.asList("met1", "vc1", "vc2"), x.getMetrics());

    // override vc
    SelectQuery q2 = base.withVirtualColumns(Arrays.<VirtualColumn>asList(vc3));
    RowResolver r2 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q2));

    x = (SelectQuery) ViewSupportHelper.rewrite(q2, r2);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2, vc3), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim1"), x.getDimensions());
    Assert.assertEquals(Arrays.asList("met1", "vc1", "vc2", "vc3"), x.getMetrics());

    SelectQuery q3 = base.withMetrics(Arrays.<String>asList("met2"));
    RowResolver r3 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q3));

    x = (SelectQuery) ViewSupportHelper.rewrite(q3, r3);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim1"), x.getDimensions());
    Assert.assertEquals(Arrays.asList("met2", "vc1", "vc2"), x.getMetrics());
  }
}