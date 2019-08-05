/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.lang.mutable.MutableDouble;

import java.util.Map;

public class JavaScriptAggregatorBenchmark extends SimpleBenchmark
{

  protected static final Map<String, String> scriptDoubleSum = Maps.newHashMap();
  static {
    scriptDoubleSum.put("fnAggregate", "function aggregate(current, a) { return current + a }");
    scriptDoubleSum.put("fnReset", "function reset() { return 0 }");
    scriptDoubleSum.put("fnCombine", "function combine(a,b) { return a + b }");
  }

  private static MutableDouble aggregate(TestDoubleColumnSelector selector, Aggregator<MutableDouble> agg, MutableDouble aggregate)
  {
    aggregate = agg.aggregate(aggregate);
    selector.increment();
    return aggregate;
  }

  private JavaScriptAggregator jsAggregator;
  private DoubleSumAggregator doubleAgg;
  final LoopingDoubleColumnSelector selector = new LoopingDoubleColumnSelector(new double[]{42.12f, 9f});

  @Override
  protected void setUp() throws Exception
  {
    Map<String, String> script = scriptDoubleSum;

    jsAggregator = new JavaScriptAggregator(
        Lists.asList(MetricSelectorUtils.wrap(selector), new ObjectColumnSelector[]{}),
        JavaScriptAggregatorFactory.compileScript(
            script.get("fnAggregate"),
            script.get("fnReset"),
            script.get("fnCombine")
        )
    );

    doubleAgg = DoubleSumAggregator.create(selector, null);
  }

  public double timeJavaScriptDoubleSum(int reps)
  {
    MutableDouble aggregate = null;
    for(int i = 0; i < reps; ++i) {
      aggregate = aggregate(selector, jsAggregator, aggregate);
    }
    return jsAggregator.get(aggregate);
  }

  public double timeNativeDoubleSum(int reps)
  {
    MutableDouble aggregate = null;
    for(int i = 0; i < reps; ++i) {
      aggregate = aggregate(selector, doubleAgg, aggregate);
    }
    return (Double) doubleAgg.get(aggregate);
  }

  public static void main(String[] args) throws Exception
  {
    Runner.main(JavaScriptAggregatorBenchmark.class, args);
  }

  protected static class LoopingDoubleColumnSelector extends TestDoubleColumnSelector
  {
    private final double[] doubles;
    private long index = 0;

    public LoopingDoubleColumnSelector(double[] doubles)
    {
      super(doubles);
      this.doubles = doubles;
    }

    @Override
    public Double get()
    {
      return doubles[(int) (index % doubles.length)];
    }

    @Override
    public void increment()
    {
      ++index;
      if (index < 0) {
        index = 0;
      }
    }
  }
}
