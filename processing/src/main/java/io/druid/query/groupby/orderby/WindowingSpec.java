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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprType;
import io.druid.math.expr.Parser;
import io.druid.common.Cacheable;
import io.druid.query.QueryCacheHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.DimFilterCacheHelper;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class WindowingSpec implements Cacheable
{
  private final List<String> partitionColumns;
  private final List<OrderByColumnSpec> sortingColumns;
  private final List<String> expressions;
  private final FlattenSpec flattener;

  @JsonCreator
  public WindowingSpec(
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("sortingColumns") List<OrderByColumnSpec> sortingColumns,
      @JsonProperty("expressions") List<String> expressions,
      @JsonProperty("flattener") FlattenSpec flattener
  )
  {
    this.partitionColumns = partitionColumns == null ? ImmutableList.<String>of() : partitionColumns;
    this.sortingColumns = sortingColumns == null ? ImmutableList.<OrderByColumnSpec>of() : sortingColumns;
    this.expressions = expressions == null ? ImmutableList.<String>of() : expressions;
    this.flattener = flattener;
  }

  public WindowingSpec(
      List<String> partitionColumns,
      List<OrderByColumnSpec> sortingColumns,
      String... expressions
  )
  {
    this(partitionColumns, sortingColumns, Arrays.asList(expressions), null);
  }

  @JsonProperty
  public List<String> getPartitionColumns()
  {
    return partitionColumns;
  }

  @JsonProperty
  public List<OrderByColumnSpec> getSortingColumns()
  {
    return sortingColumns;
  }

  @JsonProperty
  public List<String> getExpressions()
  {
    return expressions;
  }

  @JsonProperty
  public FlattenSpec getFlattener()
  {
    return flattener;
  }

  public PartitionEvaluator toEvaluator(
      List<AggregatorFactory> factories,
      List<PostAggregator> postAggregators
  )
  {
    final Map<String, ExprType> expectedTypes = Maps.newHashMap();
    for (AggregatorFactory factory : factories) {
      expectedTypes.put(factory.getName(), ExprType.bestEffortOf(factory.getTypeName()));
    }
    //todo provide output type for post aggregator
    for (PostAggregator postAggregator : postAggregators) {
      expectedTypes.put(postAggregator.getName(), ExprType.DOUBLE);
    }

    final List<Pair<String, Expr>> assigns = Lists.newArrayList();
    for (String expression : expressions) {
      assigns.add(Evals.splitAssign(expression));
    }

    PartitionEvaluator evaluator = assigns.isEmpty() ? new DummyPartitionEvaluator() : new PartitionEvaluator()
    {
      @Override
      public List<Row> evaluate(Object[] partitionKey, final List<Row> partition)
      {
        WindowContext context = new WindowContext(partition, expectedTypes);
        for (Pair<String, Expr> assign : assigns) {
          final String[] split = assign.lhs.split(":");
          final String output = split[0];
          final int[] window = toEvalWindow(split, partition.size());
          for (context.index = window[0]; context.index < window[1]; context.index++) {
            ExprEval eval = assign.rhs.eval(context);
            context.expectedTypes.put(output, eval.type());
            Map<String, Object> event = ((MapBasedRow) partition.get(context.index)).getEvent();
            event.put(output, eval.value());
          }
          Parser.reset(assign.rhs);
        }
        return partition;
      }
    };
    return flattener == null ? evaluator
                             : chain(evaluator, flattener.toEvaluator(partitionColumns, sortingColumns));
  }

  private int[] toEvalWindow(final String[] split, final int limit)
  {
    if (split.length == 1) {
      return new int[]{0, limit};
    }
    int index = Integer.valueOf(split[1]);
    if (index < 0) {
      index = limit + index;
    }
    int start = index;
    if (start < 0 || start >= limit) {
      throw new IllegalArgumentException("invalid window start " + start + "/" + limit);
    }
    int end = start + 1;
    if (split.length > 2) {
      end = start + Integer.valueOf(split[2]);
    }
    if (end < 0 || end > limit) {
      throw new IllegalArgumentException("invalid window end " + end + "/" + limit);
    }
    if (start > end) {
      throw new IllegalArgumentException("invalid window " + start + " ~ " + end);
    }
    return new int[]{start, end};
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] partitionColumnsBytes = QueryCacheHelper.computeCacheBytes(partitionColumns);
    byte[] sortingColumnsBytes = QueryCacheHelper.computeAggregatorBytes(sortingColumns);
    byte[] expressionsBytes = QueryCacheHelper.computeCacheBytes(expressions);
    byte[] flattenerBytes = QueryCacheHelper.computeCacheBytes(flattener);

    int length = 3 + partitionColumnsBytes.length
                 + sortingColumnsBytes.length
                 + expressionsBytes.length
                 + flattenerBytes.length;

    return ByteBuffer.allocate(length)
                     .put(partitionColumnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(sortingColumnsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(expressionsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(flattenerBytes)
                     .array();
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

    WindowingSpec that = (WindowingSpec) o;

    if (!partitionColumns.equals(that.partitionColumns)) {
      return false;
    }
    if (!sortingColumns.equals(that.sortingColumns)) {
      return false;
    }
    if (!expressions.equals(that.expressions)) {
      return false;
    }
    if (!Objects.equals(flattener, that.flattener)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = partitionColumns.hashCode();
    result = 31 * result + sortingColumns.hashCode();
    result = 31 * result + expressions.hashCode();
    result = 31 * result + (flattener == null ? 0 : flattener.hashCode());
    return result;
  }

  @Override
  public String toString()
  {
    return "WindowingSpec{" +
           "partitionColumns=" + partitionColumns +
           ", sortingColumns=" + sortingColumns +
           ", expressions=" + expressions +
           ", flattener='" + flattener +
           '}';
  }

  public static interface PartitionEvaluator
  {
    List<Row> evaluate(Object[] partitionKey, List<Row> partition);
  }

  private static class DummyPartitionEvaluator implements PartitionEvaluator
  {
    @Override
    public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
    {
      return partition;
    }
  }

  private PartitionEvaluator chain(final PartitionEvaluator... evaluators)
  {
    return new PartitionEvaluator()
    {
      @Override
      public List<Row> evaluate(Object[] partitionKey, List<Row> partition)
      {
        for (PartitionEvaluator evaluator : evaluators) {
          partition = evaluator.evaluate(partitionKey, partition);
        }
        return partition;
      }
    };
  }

  private static class WindowContext implements Expr.WindowContext
  {
    private final int length;
    private final List<Row> partition;
    private final Map<String, ExprType> expectedTypes;
    private int index;

    private WindowContext(List<Row> partition, Map<String, ExprType> expectedTypes)
    {
      this.length = partition.size();
      this.partition = partition;
      this.expectedTypes = expectedTypes;
    }

    @Override
    public ExprType type(String name)
    {
      return expectedTypes.get(name);
    }

    @Override
    public Object get(final int index, final String name)
    {
      return index >= 0 && index < length ? partition.get(index).getRaw(name) : null;
    }

    @Override
    public Iterable<Object> iterator(final String name)
    {
      return Iterables.transform(partition, accessFunction(name));
    }

    @Override
    public Iterable<Object> iterator(final int startRel, final int endRel, final String name)
    {
      List<Row> target;
      if (startRel > endRel) {
        target = partition.subList(Math.max(0, index + endRel), Math.min(length, index + startRel + 1));
        target = Lists.reverse(target);
      } else {
        target = partition.subList(Math.max(0, index + startRel), Math.min(length, index + endRel + 1));
      }
      return Iterables.transform(target, accessFunction(name));
    }

    @Override
    public Collection<String> names()
    {
      return null;
    }

    @Override
    public Object get(final String name)
    {
      return partition.get(index).getRaw(name);
    }

    @Override
    public int size()
    {
      return length;
    }

    @Override
    public int index()
    {
      return index;
    }

    private Function<Row, Object> accessFunction(final String name)
    {
      return new Function<Row, Object>()
      {
        @Override
        public Object apply(Row input)
        {
          return input.getRaw(name);
        }
      };
    }
  }
}
