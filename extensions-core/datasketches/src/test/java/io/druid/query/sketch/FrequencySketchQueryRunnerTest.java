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
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import com.yahoo.sketches.frequencies.ItemsSketch;
import io.druid.data.ValueType;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.datasketches.theta.SketchModule;
import io.druid.query.aggregation.datasketches.theta.SketchOperations;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class FrequencySketchQueryRunnerTest
{
  private static final SketchQueryQueryToolChest toolChest = new SketchQueryQueryToolChest(
      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
  );

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        QueryRunnerTestHelper.makeQueryRunners(
            new SketchQueryRunnerFactory(
                toolChest,
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
  }

  private final QueryRunner<Result<Map<String, Object>>> runner;

  @SuppressWarnings("unchecked")
  public FrequencySketchQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  public void testSketchResultSerDe() throws Exception
  {
    SketchHandler handler = SketchOp.FREQUENCY.handler();
    ObjectMapper mapper = TestHelper.JSON_MAPPER;
    for (Module module : new SketchModule().getJacksonModules()) {
      mapper = mapper.registerModule(module);
    }
    int nomEntries = 16;
    TypedSketch union1 = handler.newUnion(nomEntries, ValueType.STRING, null);
    handler.updateWithValue(union1, "automotive");
    handler.updateWithValue(union1, "business");
    handler.updateWithValue(union1, "entertainment");
    handler.updateWithValue(union1, "health");
    handler.updateWithValue(union1, "mezzanine");
    handler.updateWithValue(union1, "news");
    ItemsSketch sketch1 = (ItemsSketch) handler.toSketch(union1).value();
    TypedSketch union2 = handler.newUnion(nomEntries, ValueType.STRING, null);
    handler.updateWithValue(union2, "automotive1");
    handler.updateWithValue(union2, "automotive2");
    handler.updateWithValue(union2, "automotive3");
    handler.updateWithValue(union2, "business1");
    handler.updateWithValue(union2, "business2");
    handler.updateWithValue(union2, "business3");
    handler.updateWithValue(union2, "entertainment1");
    handler.updateWithValue(union2, "entertainment2");
    handler.updateWithValue(union2, "entertainment3");
    handler.updateWithValue(union2, "health1");
    handler.updateWithValue(union2, "health2");
    handler.updateWithValue(union2, "health3");
    handler.updateWithValue(union2, "mezzanine1");
    handler.updateWithValue(union2, "mezzanine2");
    handler.updateWithValue(union2, "mezzanine3");
    handler.updateWithValue(union2, "news1");
    handler.updateWithValue(union2, "news2");
    handler.updateWithValue(union2, "news3");
    handler.updateWithValue(union2, "premium1");
    handler.updateWithValue(union2, "premium2");
    handler.updateWithValue(union2, "premium3");
    ItemsSketch sketch2 = (ItemsSketch) handler.toSketch(union2).value();

    Map<String, Object> sketches = ImmutableMap.<String, Object>of("quality1", sketch1, "quality2", sketch2);
    Result<Map<String, Object>> result = new Result<>(new DateTime("2016-12-14T16:08:00"), sketches);

    String serialized = mapper.writeValueAsString(result);
    Assert.assertEquals(
        "{\"timestamp\":\"2016-12-14T16:08:00.000Z\","
        + "\"result\":{"
        + "\"quality1\":\"BAEKBAMAAAAGAAAAAAAAAAYAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAANAAAAZW50ZXJ0YWlubWVudAkAAABtZXp6YW5pbmUEAAAAbmV3cwgAAABidXNpbmVzcwoAAABhdXRvbW90aXZlBgAAAGhlYWx0aA==\","
        + "\"quality2\":\"BAEKBAQAAAAIAAAAAAAAABUAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAACAAAAHByZW1pdW0xCgAAAG1lenphbmluZTIFAAAAbmV3czMFAAAAbmV3czIIAAAAcHJlbWl1bTIKAAAAbWV6emFuaW5lMwUAAABuZXdzMQgAAABwcmVtaXVtMw==\"}}",
        serialized
    );
    Result<Map<String, Object>> deserialized = mapper.readValue(
        serialized,
        new TypeReference<Result<Map<String, Object>>>()
        {
        }
    );
    assertEqual(sketch1, SketchOperations.deserializeFrequency(deserialized.getValue().get("quality1"), ValueType.STRING));
    assertEqual(sketch2, SketchOperations.deserializeFrequency(deserialized.getValue().get("quality2"), ValueType.STRING));

    Map<String, Object> object = ImmutableMap.<String, Object>builder()
                .put("queryType", "sketch")
                .put("dataSource", "ds")
                .put("intervals", new Interval("2000-01-01/2010-01-01"))
                .put("dimensions", Arrays.asList("partCol"))
                .put("sketchOp", "FREQUENCY")
                .put(
                    "context",
                    ImmutableMap.of(
                        "postProcessing",
                        ImmutableMap.of(
                            "type", "sketch.quantiles",
                            "op", "QUANTILES",
                            "evenSpaced", 10
                        )
                    )
                )
                .build();

    System.out.println(mapper.convertValue(object, Query.class));
  }

  @Test
  public void testSketchMergeFunction() throws Exception
  {
    SketchHandler handler = SketchOp.FREQUENCY.handler();
    int nomEntries = 16;
    TypedSketch union1 = handler.newUnion(nomEntries, ValueType.STRING, null);
    handler.updateWithValue(union1, "automotive");
    handler.updateWithValue(union1, "business");
    handler.updateWithValue(union1, "entertainment");
    handler.updateWithValue(union1, "health");
    handler.updateWithValue(union1, "mezzanine");
    handler.updateWithValue(union1, "news");
    TypedSketch<ItemsSketch> sketch1 = (TypedSketch<ItemsSketch>) handler.toSketch(union1);
    Assert.assertEquals(1L, sketch1.value().getEstimate("mezzanine"));
    TypedSketch union2 = handler.newUnion(nomEntries, ValueType.STRING, null);
    handler.updateWithValue(union2, "premium");
    handler.updateWithValue(union2, "premium");
    handler.updateWithValue(union2, "premium");
    handler.updateWithValue(union2, "mezzanine");
    handler.updateWithValue(union2, "mezzanine");
    TypedSketch<ItemsSketch> sketch2 = (TypedSketch<ItemsSketch>) handler.toSketch(union2);
    Assert.assertEquals(2L, sketch2.value().getEstimate("mezzanine"));

    Result<Map<String, Object>> merged =
        new SketchBinaryFn(nomEntries, handler).apply(
            new Result<Map<String, Object>>(
                new DateTime(0),
                    ImmutableMap.<String, Object>of("quality", sketch1)
            ),
            new Result<Map<String, Object>>(
                new DateTime(0),
                    ImmutableMap.<String, Object>of("quality", sketch2)
            )
        );

    TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) merged.getValue().get("quality");
    Assert.assertEquals(3L, sketch.value().getEstimate("mezzanine"));
  }

  @Test
  public void testSketchQuery() throws Exception
  {
    SketchQuery baseQuery = new SketchQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        null,
        null,
        DefaultDimensionSpec.toSpec("market", "quality"),
        null, 16, SketchOp.FREQUENCY, null
    );

    for (boolean includeMetric : new boolean[] {false, true}) {
      SketchQuery query = baseQuery.withOverriddenContext(
          ImmutableMap.<String, Object>of(Query.ALL_METRICS_FOR_EMPTY, includeMetric)
      );
      List<Result<Map<String, Object>>> result = Sequences.toList(
          runner.run(query, null),
          Lists.<Result<Map<String, Object>>>newArrayList()
      );
      Assert.assertEquals(1, result.size());
      Map<String, Object> values = result.get(0).getValue();
      TypedSketch<ItemsSketch> sketch1 = (TypedSketch<ItemsSketch>) values.get("market");
      System.out.println(sketch1);
      Assert.assertEquals(187L, sketch1.value().getEstimate("upfront"), 5);
      Assert.assertEquals(838L, sketch1.value().getEstimate("spot"), 5);
      Assert.assertEquals(187L, sketch1.value().getEstimate("total_market"), 5);

      TypedSketch<ItemsSketch> sketch2 = (TypedSketch<ItemsSketch>) values.get("quality");
      System.out.println(sketch2);
      Assert.assertEquals(94L, sketch2.value().getEstimate("entertainment"), 5);
      Assert.assertEquals(280L, sketch2.value().getEstimate("mezzanine"), 5);
      Assert.assertEquals(280L, sketch2.value().getEstimate("premium"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("business"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("news"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("technology"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("automotive"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("travel"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("health"), 5);
    }
  }

    @Test
  public void testSketchQueryWithFilter() throws Exception
  {
    SketchQuery query = new SketchQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        AndDimFilter.of(
            BoundDimFilter.gt("market", "spot"),
            BoundDimFilter.lte("quality", "premium")
        ),
        null,
        DefaultDimensionSpec.toSpec("market", "quality"),
        null, 16, SketchOp.FREQUENCY, null
    );

    List<Result<Map<String, Object>>> result = Sequences.toList(
        runner.run(query, null),
        Lists.<Result<Map<String, Object>>>newArrayList()
    );
    Assert.assertEquals(1, result.size());
    Map<String, Object> values = result.get(0).getValue();
    TypedSketch<ItemsSketch> sketch1 = (TypedSketch<ItemsSketch>) values.get("market");
    System.out.println(sketch1);
    Assert.assertEquals(186L, sketch1.value().getEstimate("upfront"), 5);
    Assert.assertEquals(186L, sketch1.value().getEstimate("total_market"), 5);

    TypedSketch<ItemsSketch> sketch2 = (TypedSketch<ItemsSketch>) values.get("quality");
    System.out.println(sketch2);
    Assert.assertEquals(186L, sketch2.value().getEstimate("mezzanine"), 5);
    Assert.assertEquals(186L, sketch2.value().getEstimate("premium"), 5);
  }

  private void assertEqual(ItemsSketch expected, ItemsSketch result)
  {
    Assert.assertEquals(expected.toString(), result.toString());
  }
}