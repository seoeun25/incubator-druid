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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import io.druid.math.expr.Parser;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class JoinQuery<T extends Comparable<T>> extends BaseQuery<T> implements Query.RewritingQuery<T>
{
  private final JoinType joinType;
  private final List<JoinElement> elements;
  private final int numPartition;
  private final int scannerLen;
  private final int limit;
  private final int parallelism;
  private final int queue;

  private final String leftAlias;
  private final String rightAlias;

  @JsonCreator
  public JoinQuery(
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("numPartition") int numPartition,
      @JsonProperty("scannerLen") int scannerLen,
      @JsonProperty("limit") int limit,
      @JsonProperty("parallelism") int parallelism,
      @JsonProperty("queue") int queue,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(validateDataSource(elements), validateSegmentSpec(elements, querySegmentSpec), false, context);
    this.joinType = joinType == null ? JoinType.INNER : joinType;
    this.elements = validateJoinCondition(elements);
    this.numPartition = numPartition == 0 && scannerLen == 0 ? 1 : numPartition;
    this.scannerLen = scannerLen;
    this.limit = limit;
    this.parallelism = parallelism;   // warn : can take "(n-way + 1) x parallelism" threads
    this.queue = queue;
    this.leftAlias = Iterables.getOnlyElement(elements.get(0).getDataSource().getNames());
    this.rightAlias = Iterables.getOnlyElement(elements.get(1).getDataSource().getNames());
    Preconditions.checkArgument(
        numPartition > 0 || scannerLen > 0, "one of 'numPartition' or 'scannerLen' should be configured"
    );
  }

  // dummy datasource for authorization
  private static DataSource validateDataSource(List<JoinElement> elements)
  {
    Preconditions.checkArgument(elements.size() == 2, "For now");
    Set<String> names = Sets.newLinkedHashSet();
    names.addAll(elements.get(0).getDataSource().getNames());
    names.addAll(elements.get(1).getDataSource().getNames());
    return UnionDataSource.of(names);
  }

  private static QuerySegmentSpec validateSegmentSpec(List<JoinElement> elements, QuerySegmentSpec segmentSpec)
  {
    Preconditions.checkArgument(elements.size() == 2, "For now");
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    if (lhs.getDataSource() instanceof QueryDataSource) {
      Query query1 = ((QueryDataSource) lhs.getDataSource()).getQuery();
      QuerySegmentSpec segmentSpec1 = query1.getQuerySegmentSpec();
      if (segmentSpec != null && segmentSpec1.getIntervals().isEmpty()) {
        elements.set(0, lhs.withDataSource(new QueryDataSource(query1.withQuerySegmentSpec(segmentSpec))));
      } else {
        Preconditions.checkArgument(segmentSpec == null || segmentSpec.equals(segmentSpec1));
      }
      segmentSpec = segmentSpec1;
    }
    if (rhs.getDataSource() instanceof QueryDataSource) {
      Query query2 = ((QueryDataSource) rhs.getDataSource()).getQuery();
      QuerySegmentSpec segmentSpec2 = query2.getQuerySegmentSpec();
      if (segmentSpec != null && segmentSpec2.getIntervals().isEmpty()) {
        elements.set(1, lhs.withDataSource(new QueryDataSource(query2.withQuerySegmentSpec(segmentSpec))));
      } else {
        Preconditions.checkArgument(segmentSpec == null || segmentSpec.equals(segmentSpec2));
      }
      segmentSpec = segmentSpec2;
    }
    return segmentSpec;
  }

  private List<JoinElement> validateJoinCondition(List<JoinElement> elements)
  {
    Preconditions.checkArgument(elements.size() == 2, "For now");
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    Preconditions.checkArgument(lhs.getJoinExpressions().size() > 0);
    Preconditions.checkArgument(rhs.getJoinExpressions().size() > 0);
    Preconditions.checkArgument(lhs.getJoinExpressions().size() == rhs.getJoinExpressions().size());

    for (String expression : lhs.getJoinExpressions()) {
      Preconditions.checkArgument(Parser.findRequiredBindings(expression).size() == 1);
    }
    for (String expression : rhs.getJoinExpressions()) {
      Preconditions.checkArgument(Parser.findRequiredBindings(expression).size() == 1);
    }

    // todo: nested element with multiple datasources (union, etc.)
    Preconditions.checkArgument(lhs.getDataSource().getNames().size() == 1);
    Preconditions.checkArgument(rhs.getDataSource().getNames().size() == 1);

    return elements;
  }

  @JsonProperty
  public List<JoinElement> getElements()
  {
    return elements;
  }

  @JsonProperty
  public int getNumPartition()
  {
    return numPartition;
  }

  @JsonProperty
  public int getScannerLen()
  {
    return scannerLen;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty
  public int getParallelism()
  {
    return parallelism;
  }

  @JsonProperty
  public int getQueue()
  {
    return queue;
  }

  @Override
  public boolean hasFilters()
  {
    for (JoinElement element : elements) {
      if (element.hasFilter()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getType()
  {
    return Query.JOIN;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query<T> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new JoinQuery(
        joinType,
        elements,
        getQuerySegmentSpec(),
        numPartition,
        scannerLen,
        limit,
        parallelism,
        queue,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public Query<T> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new IllegalStateException();
  }

  @Override
  public Query<T> withDataSource(DataSource dataSource)
  {
    throw new IllegalStateException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    JoinPartitionSpec partitions = partition(segmentWalker, jsonMapper);
    Map<String, Object> joinProcessor = Maps.newHashMap();

    joinProcessor.put(
        "postProcessing",
        ImmutableMap.builder()
                    .put("type", "join")
                    .put("joinType", joinType.name())
                    .put("leftAlias", leftAlias)
                    .put("leftJoinExpressions", lhs.getJoinExpressions())
                    .put("rightAlias", rightAlias)
                    .put("rightJoinExpressions", rhs.getJoinExpressions())
                    .build()
    );
    Map<String, Object> context = getContext();
    Map<String, Object> joinContext = computeOverridenContext(joinProcessor);

    QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    if (partitions == null || partitions.size() == 1) {
      Query left = lhs.toQuery(segmentSpec);
      Query right = rhs.toQuery(segmentSpec);
      return new JoinDelegate(Arrays.asList(left, right), limit, parallelism, queue, joinContext);
    }
    List<Query> queries = Lists.newArrayList();
    for (Pair<DimFilter, DimFilter> filters : partitions) {
      List<Query> list = Arrays.asList(lhs.toQuery(segmentSpec, filters.lhs), rhs.toQuery(segmentSpec, filters.rhs));
      queries.add(new JoinDelegate(list, -1, 0, 0, joinContext));
    }
    return new UnionAllQuery(null, queries, false, limit, parallelism, queue, context);
  }

  private JoinPartitionSpec partition(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    if (numPartition <= 1 && scannerLen <= 0) {
      return null;
    }
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    String left = lhs.getJoinExpressions().get(0);
    String right = rhs.getJoinExpressions().get(0);

    List<String> partitions;
    if (joinType != JoinType.RO) {
      partitions = runSketchQuery(segmentWalker, lhs.getFilter(), jsonMapper, leftAlias, left);
    } else {
      partitions = runSketchQuery(segmentWalker, rhs.getFilter(), jsonMapper, rightAlias, right);
    }
    if (partitions != null && partitions.size() > 2) {
      return new JoinPartitionSpec(left, right, partitions);
    }
    return null;
  }

  private List<String> runSketchQuery(
      QuerySegmentWalker segmentWalker,
      DimFilter filter,
      ObjectMapper jsonMapper,
      String table,
      String expression
  )
  {
    return QueryUtils.runSketchQuery(
        segmentWalker,
        jsonMapper,
        getQuerySegmentSpec(),
        filter,
        table,
        expression,
        numPartition,
        scannerLen
    );
  }

  @Override
  public String toString()
  {
    return "JoinQuery{" +
           "joinType=" + joinType +
           ", elements=" + elements +
           ", numPartition=" + numPartition +
           ", scannerLen=" + scannerLen +
           ", limit=" + limit +
           '}';
  }

  private static class JoinPartitionSpec implements Iterable<Pair<DimFilter, DimFilter>>
  {
    final String leftExpression;
    final String rightExpression;
    final List<String> partitions;

    private JoinPartitionSpec(
        String leftExpression,
        String rightExpression,
        List<String> partitions
    )
    {
      this.leftExpression = Preconditions.checkNotNull(leftExpression);
      this.rightExpression = Preconditions.checkNotNull(rightExpression);
      this.partitions = Preconditions.checkNotNull(partitions);
    }

    @Override
    public Iterator<Pair<DimFilter, DimFilter>> iterator()
    {
      return new Iterator<Pair<DimFilter, DimFilter>>()
      {
        private final Iterator<DimFilter> left = QueryUtils.toFilters(leftExpression, partitions).iterator();
        private final Iterator<DimFilter> right = QueryUtils.toFilters(rightExpression, partitions).iterator();

        @Override
        public boolean hasNext()
        {
          return left.hasNext();
        }

        @Override
        public Pair<DimFilter, DimFilter> next()
        {
          return Pair.of(left.next(), right.next());
        }
      };
    }

    public int size()
    {
      return partitions.size();
    }
  }

  @SuppressWarnings("unchecked")
  public static class JoinDelegate extends UnionAllQuery
  {
    public JoinDelegate(List<Query> list, int limit, int parallelism, int queue, Map<String, Object> context)
    {
      super(null, list, false, limit, parallelism, queue, context);
    }

    @Override
    public Query withQueries(List queries)
    {
      return new JoinDelegate(queries, getLimit(), getParallelism(), getQueue(), getContext());
    }

    @Override
    public Query withQuery(Query query)
    {
      throw new IllegalStateException();
    }

    @Override
    public Query withOverriddenContext(Map contextOverride)
    {
      Map<String, Object> context = computeOverridenContext(contextOverride);
      return new JoinDelegate(getQueries(), getLimit(), getParallelism(), getQueue(), context);
    }

    @Override
    public String toString()
    {
      return "JoinDelegate{" +
             "queries=" + getQueries() +
             ", limit=" + getLimit() +
             ", join=" + getContextValue("postProcessing") +
             '}';
    }
  }
}
