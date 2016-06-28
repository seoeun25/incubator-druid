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

package io.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.CharSource;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.MapVirtualColumn;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.VirtualColumn;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.druid.query.QueryRunnerTestHelper.dataSource;
import static io.druid.query.QueryRunnerTestHelper.dayGran;
import static io.druid.query.QueryRunnerTestHelper.fullOnInterval;
import static io.druid.query.QueryRunnerTestHelper.makeSegmentQueryRunner;
import static io.druid.query.QueryRunnerTestHelper.transformToConstructionFeeder;

/**
 */
@RunWith(Parameterized.class)
public class VirtualColumnTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final StupidPool<ByteBuffer> pool = new StupidPool<>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );

    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);

    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(
        configSupplier, mapper, engine, TestQueryRunners.pool,
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );
    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        configSupplier,
        toolChest,
        TestQueryRunners.pool
    );

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime("2011-01-12T00:00:00.000Z").getMillis())
        .withQueryGranularity(QueryGranularities.NONE)
        .build();
    final IncrementalIndex index = new OnheapIncrementalIndex(schema, true, 10000);

    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new TimestampSpec("ts", "iso", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(Arrays.asList("dim", "keys", "values", "value")), null, null),
            "\t",
            ",",
            Arrays.asList("ts", "dim", "keys", "values", "value")
        )
        , "utf8"
    );

    CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\t100,200,300\t100\n" +
        "2011-01-12T00:00:00.000Z\tc\tkey1,key2\t100,500,900\t200\n" +
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\t400,500,600\t300\n" +
        "2011-01-12T00:00:00.000Z\t\tkey1,key2,key3\t10,20,30\t400\n" +
        "2011-01-12T00:00:00.000Z\tc\tkey1,key2,key3\t1,5,9\t500\n" +
        "2011-01-12T00:00:00.000Z\t\tkey1,key2,key3\t2,4,8\t600\n"
    );

    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(index, input, parser);
    QueryableIndex index2 = TestIndex.persistRealtimeAndLoadMMapped(index1);

    final ExecutorService executorService = MoreExecutors.sameThreadExecutor();
    final List<QueryRunner<Row>> runners = Arrays.asList(
        makeSegmentQueryRunner(factory, "index1", new IncrementalIndexSegment(index1, "index1")),
        makeSegmentQueryRunner(factory, "index2", new QueryableIndexSegment("index2", index2))
    );
    return transformToConstructionFeeder(
        Lists.transform(
            runners, new Function<QueryRunner, QueryRunner>()
            {
              @Override
              public QueryRunner apply(QueryRunner input)
              {
                return new FinalizeResultsQueryRunner(
                    factory.mergeRunners(executorService, Arrays.<QueryRunner<Row>>asList(input)),
                    toolChest
                );
              }
            }
        )
    );
  }

  private final QueryRunner runner;

  public VirtualColumnTest(QueryRunner runner)
  {
    this.runner = runner;
  }

  private GroupByQuery.Builder testBuilder()
  {
    return GroupByQuery.builder()
                       .setDataSource(dataSource)
                       .setGranularity(dayGran)
                       .setInterval(fullOnInterval);
  }

  @Test
  public void testBasic() throws Exception
  {
    GroupByQuery.Builder builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "dim_nvl", "sum_of_key1", "count"},
        new Object[]{"2011-01-12T00:00:00.000Z", "a", 500L, 2L},
        new Object[]{"2011-01-12T00:00:00.000Z", "c", 101L, 2L},
        new Object[]{"2011-01-12T00:00:00.000Z", "null", 12L, 2L}
    );

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new MapVirtualColumn("keys", "values", "params"),
        new ExprVirtualColumn("nvl(dim, 'null')", "dim_nvl")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("dim_nvl"))
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("sum_of_key1", null, "cast(params.key1, 'long')", null),
                new CountAggregatorFactory("count")
            )
        )
        .setVirtualColumns(virtualColumns)
        .addOrderByColumn("dim_nvl")
        .build();
    checkSelectQuery(query, expectedResults);
  }

  @Test
  public void testDimensionToMetric() throws Exception
  {
    GroupByQuery.Builder builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "sum_of_key1", "sum_of_key2", "count"},
        new Object[]{"2011-01-12T00:00:00.000Z", 2100L, 2100L, 6L}
    );

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new ExprVirtualColumn("cast(value, 'long')", "val_long")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec())
        .setAggregatorSpecs(
            Arrays.asList(
                new LongSumAggregatorFactory("sum_of_key1", "val_long"),
                new LongSumAggregatorFactory("sum_of_key2", null, "cast(value, 'long')", null),
                new CountAggregatorFactory("count")
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();
    checkSelectQuery(query, expectedResults);
  }

  @Test
  public void testX() throws Exception
  {
    GroupByQuery.Builder builder = testBuilder();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "keys", "cardinality1", "cardinality2"},
        new Object[]{"2011-01-12T00:00:00.000Z", "key1", 3.0021994137521975D, 6.008806266444944D},
        new Object[]{"2011-01-12T00:00:00.000Z", "key2", 3.0021994137521975D, 6.008806266444944D},
        new Object[]{"2011-01-12T00:00:00.000Z", "key3", 2.000977198748901D, 5.006113467958146D}
    );

    List<VirtualColumn> virtualColumns = Arrays.<VirtualColumn>asList(
        new ExprVirtualColumn("if (value < '300', value, 0)", "val_expr")
    );
    GroupByQuery query = builder
        .setDimensions(DefaultDimensionSpec.toSpec("keys"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                new CardinalityAggregatorFactory("cardinality1", Arrays.asList("val_expr"), true),
                new CardinalityAggregatorFactory("cardinality2", Arrays.asList("value"), true)
            )
        )
        .setVirtualColumns(virtualColumns)
        .build();
    checkSelectQuery(query, expectedResults);
  }

  private void checkSelectQuery(GroupByQuery query, List<Row> expected) throws Exception
  {
    List<Row> results = Sequences.toList(
        runner.run(query, ImmutableMap.of()),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expected, results, "");
  }
}