/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cuboids
{
  public static final String COUNT_ALL_METRIC = "$$";
  public static final Set<String> BASIC_AGGREGATORS = ImmutableSet.of("count", "min", "max", "sum");

  public static final int PAGES = 16;
  public static final int BUFFER_SIZE = 256 << 10;

  private static final Logger LOG = new Logger(Cuboids.class);
  private static final Pattern CUBOID = Pattern.compile("^___(\\d+)_(.+)___(.*)?$");

  public static String dimension(long cubeId, String columnName)
  {
    return String.format("___%d_%s___", cubeId, columnName);
  }

  public static String metric(long cubeId, String columnName, String aggregation)
  {
    return String.format("___%d_%s___%s", cubeId, columnName, aggregation);
  }

  static Map<Long, CuboidSpec> extractCuboids(Iterable<String> values)
  {
    final Map<Long, CuboidSpec> cuboids = Maps.newHashMap();
    final Matcher matcher = CUBOID.matcher("");
    for (String value : values) {
      matcher.reset(value);
      if (matcher.matches()) {
        long cubeId = Long.parseLong(matcher.group(1));
        String columnName = matcher.group(2);
        String aggregator = matcher.group(3);
        CuboidSpec cuboid = cuboids.computeIfAbsent(cubeId, new Function<Long, CuboidSpec>()
        {
          @Override
          public CuboidSpec apply(Long cubeId)
          {
            return new CuboidSpec(null, Lists.newArrayList(), Maps.newHashMap());
          }
        });
        if (StringUtils.isNullOrEmpty(aggregator)) {
          cuboid.getDimensions().add(columnName);
        } else {
          cuboid.getMetrics().computeIfAbsent(
              columnName, new Function<String, Set<String>>()
              {
                @Override
                public Set<String> apply(String s) { return Sets.newHashSet();}
              }
          ).add(aggregator);
        }
      }
    }
    return cuboids;
  }

  // hate this
  static AggregatorFactory convert(String aggregator, String name, String fieldName, ValueDesc inputType)
  {
    switch (aggregator) {
      case "count":
        return new CountAggregatorFactory(name, null, fieldName);
      case "min":
        return new GenericMinAggregatorFactory(name, fieldName, inputType);
      case "max":
        return new GenericMaxAggregatorFactory(name, fieldName, inputType);
      case "sum":
        return new GenericSumAggregatorFactory(name, fieldName, inputType);
    }
    LOG.warn("Not supported cube aggregator [%s]", aggregator);
    return null;
  }

  public static boolean supports(Map<String, Set<String>> metrics, List<AggregatorFactory> aggregators)
  {
    for (AggregatorFactory aggregator : aggregators) {
      if (!supports(metrics, aggregator)) {
        return false;
      }
    }
    return true;
  }

  private static boolean supports(Map<String, Set<String>> metrics, AggregatorFactory aggregator)
  {
    Set<String> supports = null;
    if (AggregatorFactory.isCountAll(aggregator)) {
      supports = metrics.get(COUNT_ALL_METRIC);
    } else if (aggregator instanceof AggregatorFactory.SingleFielded) {
      supports = metrics.get(((AggregatorFactory.SingleFielded) aggregator).getFieldName());
    }
    if (supports == null) {
      return false;
    }
    if (aggregator instanceof CountAggregatorFactory) {
      return supports.contains("count") && ((CountAggregatorFactory) aggregator).getPredicate() == null;
    }
    if (aggregator instanceof GenericMinAggregatorFactory) {
      return supports.contains("min") && ((GenericMinAggregatorFactory) aggregator).getPredicate() == null;
    }
    if (aggregator instanceof GenericMaxAggregatorFactory) {
      return supports.contains("max") && ((GenericMaxAggregatorFactory) aggregator).getPredicate() == null;
    }
    if (aggregator instanceof GenericSumAggregatorFactory) {
      return supports.contains("sum") && ((GenericSumAggregatorFactory) aggregator).getPredicate() == null;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static <T extends BaseAggregationQuery> T rewrite(T query)
  {
    final List<AggregatorFactory> rewritten = Lists.newArrayList();
    for (AggregatorFactory aggregator : query.getAggregatorSpecs()) {
      if (AggregatorFactory.isCountAll(aggregator)) {
        aggregator = new LongSumAggregatorFactory(aggregator.getName(), COUNT_ALL_METRIC);
      } else {
        aggregator = aggregator.getCombiningFactory();
      }
      rewritten.add(aggregator);
    }
    return (T) query.withAggregatorSpecs(rewritten);
  }
}