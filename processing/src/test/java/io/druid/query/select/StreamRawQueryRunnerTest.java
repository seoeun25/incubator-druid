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
import com.metamx.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.TestIndex;
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
public class StreamRawQueryRunnerTest extends QueryRunnerTestHelper
{
  static final Map<String, Object> CONTEXT = ImmutableMap.of();
  static final QuerySegmentSpec I_0112_0114 = new LegacySegmentSpec(
      new Interval("2011-01-12/2011-01-14")
  );

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public StreamRawQueryRunnerTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  private Druids.SelectQueryBuilder newTestQuery()
  {
    return Druids.newSelectQueryBuilder()
                 .dataSource(dataSource)
                 .dimensionSpecs(DefaultDimensionSpec.toSpec(Arrays.<String>asList()))
                 .metrics(Arrays.<String>asList())
                 .intervals(QueryRunnerTestHelper.fullOnInterval)
                 .granularity(QueryRunnerTestHelper.allGran);
  }

  @Test
  public void testBasic()
  {
    Druids.SelectQueryBuilder builder = testEq(newTestQuery());
    testEq(builder.columns(Arrays.asList("market", "quality")));
    testEq(builder.columns(Arrays.asList("__time", "market", "quality", "index", "indexMin")));
    testEq(builder.intervals(firstToThird));
    testEq(builder.limit(32));

    StreamRawQuery query = builder.streamingRaw();

    List<Object[]> expected = Lists.newArrayList(
        array(time("2011-04-01"), "spot", "automotive", 135.88510131835938, 135.8851f),
        array(time("2011-04-01"), "spot", "business", 118.57034301757812, 118.57034f),
        array(time("2011-04-01"), "spot", "entertainment", 158.74722290039062, 158.74722f),
        array(time("2011-04-01"), "spot", "health", 120.13470458984375, 120.134705f),
        array(time("2011-04-01"), "spot", "mezzanine", 109.70581817626953, 109.70582f),
        array(time("2011-04-01"), "spot", "news", 121.58358001708984, 121.58358f),
        array(time("2011-04-01"), "spot", "premium", 144.5073699951172, 144.50737f),
        array(time("2011-04-01"), "spot", "technology", 78.62254333496094, 78.62254f),
        array(time("2011-04-01"), "spot", "travel", 119.92274475097656, 119.922745f),
        array(time("2011-04-01"), "total_market", "mezzanine", 1314.8397216796875, 1314.8397f),
        array(time("2011-04-01"), "total_market", "premium", 1522.043701171875, 1522.0437f),
        array(time("2011-04-01"), "upfront", "mezzanine", 1447.3411865234375, 1447.3412f),
        array(time("2011-04-01"), "upfront", "premium", 1234.24755859375, 1234.2476f),
        array(time("2011-04-02"), "spot", "automotive", 147.42593383789062, 147.42593f),
        array(time("2011-04-02"), "spot", "business", 112.98703002929688, 112.98703f),
        array(time("2011-04-02"), "spot", "entertainment", 166.01605224609375, 166.01605f),
        array(time("2011-04-02"), "spot", "health", 113.44600677490234, 113.44601f),
        array(time("2011-04-02"), "spot", "mezzanine", 110.93193054199219, 110.93193f),
        array(time("2011-04-02"), "spot", "news", 114.2901382446289, 114.29014f),
        array(time("2011-04-02"), "spot", "premium", 135.30149841308594, 135.3015f),
        array(time("2011-04-02"), "spot", "technology", 97.38743591308594, 97.387436f),
        array(time("2011-04-02"), "spot", "travel", 126.41136169433594, 126.41136f),
        array(time("2011-04-02"), "total_market", "mezzanine", 1193.5562744140625, 1193.5563f),
        array(time("2011-04-02"), "total_market", "premium", 1321.375, 1321.375f),
        array(time("2011-04-02"), "upfront", "mezzanine", 1144.3424072265625, 1144.3424f),
        array(time("2011-04-02"), "upfront", "premium", 1049.738525390625, 1049.7385f)
    );

    List<Object[]> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Object[]>newArrayList()
    );
    Assert.assertEquals(expected.size(), results.size());
    for (int i = 0; i < results.size(); i++) {
      Assert.assertArrayEquals(i + " th row", expected.get(i), results.get(i));
    }

    // disable split (need sketch)
    query = builder.context(ImmutableMap.<String, Object>of(Query.RAW_LOCAL_SPLIT_NUM, 1))
                   .streamingRaw(Arrays.asList("quality", "__time"));

    expected = Lists.newArrayList(
        array(time("2011-04-01"), "spot", "automotive", 135.88510131835938, 135.8851f),
        array(time("2011-04-02"), "spot", "automotive", 147.42593383789062, 147.42593f),
        array(time("2011-04-01"), "spot", "business", 118.57034301757812, 118.57034f),
        array(time("2011-04-02"), "spot", "business", 112.98703002929688, 112.98703f),
        array(time("2011-04-01"), "spot", "entertainment", 158.74722290039062, 158.74722f),
        array(time("2011-04-02"), "spot", "entertainment", 166.01605224609375, 166.01605f),
        array(time("2011-04-01"), "spot", "health", 120.13470458984375, 120.134705f),
        array(time("2011-04-02"), "spot", "health", 113.44600677490234, 113.44601f),
        array(time("2011-04-01"), "spot", "mezzanine", 109.70581817626953, 109.70582f),
        array(time("2011-04-01"), "total_market", "mezzanine", 1314.8397216796875, 1314.8397f),
        array(time("2011-04-01"), "upfront", "mezzanine", 1447.3411865234375, 1447.3412f),
        array(time("2011-04-02"), "spot", "mezzanine", 110.93193054199219, 110.93193f),
        array(time("2011-04-02"), "total_market", "mezzanine", 1193.5562744140625, 1193.5563f),
        array(time("2011-04-02"), "upfront", "mezzanine", 1144.3424072265625, 1144.3424f),
        array(time("2011-04-01"), "spot", "news", 121.58358001708984, 121.58358f),
        array(time("2011-04-02"), "spot", "news", 114.2901382446289, 114.29014f),
        array(time("2011-04-01"), "spot", "premium", 144.5073699951172, 144.50737f),
        array(time("2011-04-01"), "total_market", "premium", 1522.043701171875, 1522.0437f),
        array(time("2011-04-01"), "upfront", "premium", 1234.24755859375, 1234.2476f),
        array(time("2011-04-02"), "spot", "premium", 135.30149841308594, 135.3015f),
        array(time("2011-04-02"), "total_market", "premium", 1321.375, 1321.375f),
        array(time("2011-04-02"), "upfront", "premium", 1049.738525390625, 1049.7385f),
        array(time("2011-04-01"), "spot", "technology", 78.62254333496094, 78.62254f),
        array(time("2011-04-02"), "spot", "technology", 97.38743591308594, 97.387436f),
        array(time("2011-04-01"), "spot", "travel", 119.92274475097656, 119.922745f),
        array(time("2011-04-02"), "spot", "travel", 126.41136169433594, 126.41136f)
    );
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Object[]>newArrayList()
    );
    Assert.assertEquals(expected.size(), results.size());
    for (int i = 0; i < results.size(); i++) {
      Assert.assertArrayEquals(i + " th row", expected.get(i), results.get(i));
    }
  }

  private static long time(String time)
  {
    return new DateTime(time).getMillis();
  }

  private Druids.SelectQueryBuilder testEq(Druids.SelectQueryBuilder builder)
  {
    StreamRawQuery query1 = builder.streamingRaw();
    StreamRawQuery query2 = builder.streamingRaw();
    Map<StreamRawQuery, String> map = ImmutableMap.of(query1, query1.toString());
    Assert.assertEquals(query2.toString(), map.get(query2));
    return builder;
  }
}