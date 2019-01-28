/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.sql.calcite.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Parser;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.ExpressionHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.StreamRawQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.DimensionTopNMetricSpec;
import io.druid.query.topn.InvertedTopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNMetricSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.DimensionExpression;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.rule.GroupByRules;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * A fully formed Druid query, built from a {@link PartialDruidQuery}. The work to develop this query is done
 * during construction, which may throw {@link CannotBuildQueryException}.
 */
public class DruidBaseQuery implements DruidQuery
{
  private final DataSource dataSource;
  private final RowSignature sourceRowSignature;
  private final PlannerContext plannerContext;

  @Nullable
  private final DimFilter filter;

  @Nullable
  private final SelectProjection selectProjection;

  @Nullable
  private final Grouping grouping;

  @Nullable
  private final SortProject sortProject;

  @Nullable
  private final LimitSpec limitSpec;

  @Nullable
  private final RowSignature outputRowSignature;

  @Nullable
  private final RelDataType outputRowType;

  private final Query query;

  public DruidBaseQuery(
      final PartialDruidQuery partialQuery,
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    this.dataSource = dataSource;
    this.sourceRowSignature = sourceRowSignature;
    this.outputRowType = partialQuery.leafRel().getRowType();
    this.plannerContext = plannerContext;

    // Now the fun begins.
    this.filter = computeWhereFilter(partialQuery, sourceRowSignature, plannerContext);
    this.selectProjection = computeSelectProjection(partialQuery, plannerContext, sourceRowSignature);
    this.grouping = computeGrouping(partialQuery, plannerContext, sourceRowSignature, rexBuilder, finalizeAggregations);

    final RowSignature sortingInputRowSignature;

    if (this.selectProjection != null) {
      sortingInputRowSignature = this.selectProjection.getOutputRowSignature();
    } else if (this.grouping != null) {
      sortingInputRowSignature = this.grouping.getOutputRowSignature();
    } else {
      sortingInputRowSignature = sourceRowSignature;
    }

    this.sortProject = computeSortProject(partialQuery, plannerContext, sortingInputRowSignature);

    // outputRowSignature is used only for scan and select query, and thus sort and grouping must be null
    this.outputRowSignature = sortProject == null ? sortingInputRowSignature : sortProject.getOutputRowSignature();

    this.limitSpec = computeLimitSpec(partialQuery, sortingInputRowSignature);
    this.query = computeQuery();
  }

  @Nullable
  private static DimFilter computeWhereFilter(
      final PartialDruidQuery partialQuery,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext
  )
  {
    final Filter whereFilter = partialQuery.getWhereFilter();

    if (whereFilter == null) {
      return null;
    }

    final RexNode condition = whereFilter.getCondition();
    final DimFilter dimFilter = Expressions.toFilter(
        plannerContext,
        sourceRowSignature,
        condition
    );
    if (dimFilter == null) {
      throw new CannotBuildQueryException(whereFilter, condition);
    } else {
      return dimFilter;
    }
  }

  @Nullable
  private static SelectProjection computeSelectProjection(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature
  )
  {
    final Project project = partialQuery.getSelectProject();

    if (project == null || partialQuery.getAggregate() != null) {
      return null;
    }

    final List<DruidExpression> expressions = new ArrayList<>();

    for (final RexNode rexNode : project.getChildExps()) {
      final DruidExpression expression = Expressions.toDruidExpression(
          plannerContext,
          sourceRowSignature,
          rexNode
      );

      if (expression == null) {
        throw new CannotBuildQueryException(project, rexNode);
      } else {
        expressions.add(expression);
      }
    }

    final List<String> directColumns = new ArrayList<>();
    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final List<String> rowOrder = new ArrayList<>();

    final String virtualColumnPrefix = Calcites.findUnusedPrefix(
        "v",
        new TreeSet<>(sourceRowSignature.getRowOrder())
    );
    int virtualColumnNameCounter = 0;

    for (DruidExpression expression : expressions) {
      if (expression.isDirectColumnAccess()) {
        directColumns.add(expression.getDirectColumn());
        rowOrder.add(expression.getDirectColumn());
      } else {
        final String virtualColumnName = virtualColumnPrefix + virtualColumnNameCounter++;
        virtualColumns.add(
            expression.toVirtualColumn(
                virtualColumnName
            )
        );
        rowOrder.add(virtualColumnName);
      }
    }

    return new SelectProjection(directColumns, virtualColumns, RowSignature.from(rowOrder, project.getRowType()));
  }

  @Nullable
  private static Grouping computeGrouping(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    final Aggregate aggregate = partialQuery.getAggregate();
    final Project aggregateProject = partialQuery.getAggregateProject();

    if (aggregate == null) {
      return null;
    }

    final List<DimensionExpression> dimensions = computeDimensions(partialQuery, plannerContext, sourceRowSignature);
    final List<Aggregation> aggregations = computeAggregations(
        partialQuery,
        plannerContext,
        sourceRowSignature,
        rexBuilder,
        finalizeAggregations
    );

    final RowSignature aggregateRowSignature = RowSignature.from(
        ImmutableList.copyOf(
            Iterators.concat(
                dimensions.stream().map(DimensionExpression::getOutputName).iterator(),
                aggregations.stream().map(Aggregation::getOutputName).iterator()
            )
        ),
        aggregate.getRowType()
    );

    final HavingSpec havingFilter = computeHavingFilter(
        partialQuery,
        aggregateRowSignature,
        plannerContext
    );

    if (aggregateProject == null) {
      return Grouping.create(dimensions, aggregations, havingFilter, aggregateRowSignature);
    } else {
      final ProjectRowOrderAndPostAggregations projectRowOrderAndPostAggregations = computePostAggregations(
          plannerContext,
          aggregateRowSignature,
          aggregateProject,
          "p"
      );
      projectRowOrderAndPostAggregations.postAggregations.forEach(
          postAggregator -> aggregations.add(Aggregation.create(postAggregator))
      );

      // Remove literal dimensions that did not appear in the projection. This is useful for queries
      // like "SELECT COUNT(*) FROM tbl GROUP BY 'dummy'" which some tools can generate, and for which we don't
      // actually want to include a dimension 'dummy'.
      final ImmutableBitSet aggregateProjectBits = RelOptUtil.InputFinder.bits(aggregateProject.getChildExps(), null);
      for (int i = dimensions.size() - 1; i >= 0; i--) {
        final DimensionExpression dimension = dimensions.get(i);
        if (Evals.isConstant(Parser.parse(dimension.getDruidExpression().getExpression())) &&
            !aggregateProjectBits.get(i)) {
          dimensions.remove(i);
        }
      }

      return Grouping.create(
          dimensions,
          aggregations,
          havingFilter,
          RowSignature.from(projectRowOrderAndPostAggregations.rowOrder, aggregateProject.getRowType())
      );
    }
  }

  @Nullable
  private SortProject computeSortProject(
      PartialDruidQuery partialQuery,
      PlannerContext plannerContext,
      RowSignature sortingInputRowSignature
  )
  {
    final Project sortProject = partialQuery.getSortProject();
    if (sortProject == null) {
      return null;
    } else {
      final ProjectRowOrderAndPostAggregations projectRowOrderAndPostAggregations = computePostAggregations(
          plannerContext,
          sortingInputRowSignature,
          sortProject,
          "s"
      );

      return new SortProject(
          sortingInputRowSignature,
          projectRowOrderAndPostAggregations.postAggregations,
          RowSignature.from(projectRowOrderAndPostAggregations.rowOrder, sortProject.getRowType())
      );
    }
  }

  private static class ProjectRowOrderAndPostAggregations
  {
    private final List<String> rowOrder;
    private final List<PostAggregator> postAggregations;

    ProjectRowOrderAndPostAggregations(List<String> rowOrder, List<PostAggregator> postAggregations)
    {
      this.rowOrder = rowOrder;
      this.postAggregations = postAggregations;
    }
  }

  private static ProjectRowOrderAndPostAggregations computePostAggregations(
      PlannerContext plannerContext,
      RowSignature inputRowSignature,
      Project project,
      String basePrefix
  )
  {
    final List<String> rowOrder = new ArrayList<>();
    final List<PostAggregator> aggregations = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefix(
        basePrefix,
        new TreeSet<>(inputRowSignature.getRowOrder())
    );

    int outputNameCounter = 0;
    for (final RexNode postAggregatorRexNode : project.getChildExps()) {
      // Attempt to convert to PostAggregator.
      final DruidExpression postAggregatorExpression = Expressions.toDruidExpression(
          plannerContext,
          inputRowSignature,
          postAggregatorRexNode
      );

      if (postAggregatorExpression == null) {
        throw new CannotBuildQueryException(project, postAggregatorRexNode);
      }

      if (postAggregatorDirectColumnIsOk(inputRowSignature, postAggregatorExpression, postAggregatorRexNode)) {
        // Direct column access, without any type cast as far as Druid's runtime is concerned.
        // (There might be a SQL-level type cast that we don't care about)
        rowOrder.add(postAggregatorExpression.getDirectColumn());
      } else {
        final String postAggregatorName = outputNamePrefix + outputNameCounter++;
        final PostAggregator postAggregator = new MathPostAggregator(
            postAggregatorName,
            postAggregatorExpression.getExpression(),
            null
        );
        aggregations.add(postAggregator);
        rowOrder.add(postAggregator.getName());
      }
    }

    return new ProjectRowOrderAndPostAggregations(rowOrder, aggregations);
  }

  /**
   * Returns dimensions corresponding to {@code aggregate.getGroupSet()}, in the same order.
   *
   * @param partialQuery       partial query
   * @param plannerContext     planner context
   * @param sourceRowSignature source row signature
   *
   * @return dimensions
   *
   * @throws CannotBuildQueryException if dimensions cannot be computed
   */
  private static List<DimensionExpression> computeDimensions(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate());
    final List<DimensionExpression> dimensions = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefix("d", new TreeSet<>(sourceRowSignature.getRowOrder()));
    int outputNameCounter = 0;

    for (int i : aggregate.getGroupSet()) {
      // Dimension might need to create virtual columns. Avoid giving it a name that would lead to colliding columns.
      final String dimOutputName = outputNamePrefix + outputNameCounter++;
      final RexNode rexNode = Expressions.fromFieldAccess(sourceRowSignature, partialQuery.getSelectProject(), i);
      final DruidExpression druidExpression = Expressions.toDruidExpression(
          plannerContext,
          sourceRowSignature,
          rexNode
      );
      if (druidExpression == null) {
        throw new CannotBuildQueryException(aggregate, rexNode);
      }

      final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
      final ValueDesc outputType = Calcites.getValueDescForSqlTypeName(sqlTypeName);
      if (!ValueDesc.isPrimitive(outputType)) {
        // Can't group on unknown or COMPLEX types.
        throw new CannotBuildQueryException(aggregate, rexNode);
      }

      dimensions.add(new DimensionExpression(dimOutputName, druidExpression, outputType));
    }

    return dimensions;
  }

  /**
   * Returns aggregations corresponding to {@code aggregate.getAggCallList()}, in the same order.
   *
   * @param partialQuery         partial query
   * @param plannerContext       planner context
   * @param sourceRowSignature   source row signature
   * @param rexBuilder           calcite RexBuilder
   * @param finalizeAggregations true if this query should include explicit finalization for all of its
   *                             aggregators, where required. Useful for subqueries where Druid's native query layer
   *                             does not do this automatically.
   *
   * @return aggregations
   *
   * @throws CannotBuildQueryException if dimensions cannot be computed
   */
  private static List<Aggregation> computeAggregations(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate());
    final List<Aggregation> aggregations = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefix("a", new TreeSet<>(sourceRowSignature.getRowOrder()));

    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      final String aggName = outputNamePrefix + i;
      final AggregateCall aggCall = aggregate.getAggCallList().get(i);
      final Aggregation aggregation = GroupByRules.translateAggregateCall(
          plannerContext,
          sourceRowSignature,
          rexBuilder,
          partialQuery.getSelectProject(),
          aggCall,
          aggregations,
          aggName,
          finalizeAggregations
      );

      if (aggregation == null) {
        throw new CannotBuildQueryException(aggregate, aggCall);
      }

      aggregations.add(aggregation);
    }

    return aggregations;
  }

  @Nullable
  private static HavingSpec computeHavingFilter(
      final PartialDruidQuery partialQuery,
      final RowSignature outputRowSignature,
      final PlannerContext plannerContext
  )
  {
    final Filter havingFilter = partialQuery.getHavingFilter();

    if (havingFilter == null) {
      return null;
    }

    final RexNode condition = havingFilter.getCondition();
    final DruidExpression expression = Expressions.toDruidExpression(
        plannerContext,
        outputRowSignature,
        condition
    );
    if (expression == null) {
      throw new CannotBuildQueryException(havingFilter, condition);
    } else {
      return new ExpressionHavingSpec(expression.getExpression());
    }
  }

  @Nullable
  private static LimitSpec computeLimitSpec(
      final PartialDruidQuery partialQuery,
      final RowSignature outputRowSignature
  )
  {
    final Sort sort;

    if (partialQuery.getAggregate() == null) {
      sort = partialQuery.getSelectSort();
    } else {
      sort = partialQuery.getSort();
    }

    if (sort == null) {
      return null;
    }

    final Integer limit = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : null;
    final List<OrderByColumnSpec> orderBys = new ArrayList<>(sort.getChildExps().size());

    if (sort.offset != null) {
      // LimitSpecs don't accept offsets.
      throw new CannotBuildQueryException(sort);
    }

    // Extract orderBy column specs.
    for (int sortKey = 0; sortKey < sort.getChildExps().size(); sortKey++) {
      final RexNode sortExpression = sort.getChildExps().get(sortKey);
      final RelFieldCollation collation = sort.getCollation().getFieldCollations().get(sortKey);
      if (!sortExpression.isA(SqlKind.INPUT_REF)) {
        // We don't support sorting by anything other than refs which actually appear in the query result.
        throw new CannotBuildQueryException(sort, sortExpression);
      }
      final RexInputRef ref = (RexInputRef) sortExpression;
      final String fieldName = outputRowSignature.getRowOrder().get(ref.getIndex());
      final ValueDesc type = outputRowSignature.getColumnType(fieldName);

      final Direction direction;

      if (collation.getDirection() == RelFieldCollation.Direction.ASCENDING) {
        direction = Direction.ASCENDING;
      } else if (collation.getDirection() == RelFieldCollation.Direction.DESCENDING) {
        direction = Direction.DESCENDING;
      } else {
        throw new ISE("WTF?! Don't know what to do with direction[%s]", collation.getDirection());
      }

      // use natural ordering.. what so ever
      orderBys.add(new OrderByColumnSpec(fieldName, direction));
    }

    return new LimitSpec(orderBys, limit);
  }

  /**
   * Returns true if a post-aggregation "expression" can be realized as a direct field access. This is true if it's
   * a direct column access that doesn't require an implicit cast.
   *
   * @param aggregateRowSignature signature of the aggregation
   * @param expression            post-aggregation expression
   * @param rexNode               RexNode for the post-aggregation expression
   *
   * @return yes or no
   */
  private static boolean postAggregatorDirectColumnIsOk(
      final RowSignature aggregateRowSignature,
      final DruidExpression expression,
      final RexNode rexNode
  )
  {
    if (!expression.isDirectColumnAccess()) {
      return false;
    }

    // Check if a cast is necessary.
    final ValueDesc toExprType = aggregateRowSignature.getColumnType(expression.getDirectColumn());

    final ValueDesc fromExprType = Calcites.getValueDescForSqlTypeName(rexNode.getType().getSqlTypeName());

    return toExprType.equals(fromExprType);
  }

  private List<VirtualColumn> getVirtualColumns(final boolean includeDimensions)
  {
    final List<VirtualColumn> retVal = new ArrayList<>();

    if (selectProjection != null) {
      retVal.addAll(selectProjection.getVirtualColumns());
    } else {
      if (grouping != null) {
        if (includeDimensions) {
          for (DimensionExpression dimensionExpression : grouping.getDimensions()) {
            VirtualColumn virtualColumn = dimensionExpression.toVirtualColumn();
            if (virtualColumn != null) {
              retVal.add(virtualColumn);
            }
          }
        }

        for (Aggregation aggregation : grouping.getAggregations()) {
          retVal.addAll(aggregation.getVirtualColumns());
        }
      }
    }

    return retVal;
  }

  @Override
  public RelDataType getOutputRowType()
  {
    return outputRowType;
  }

  @Override
  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  @Override
  public Query getQuery()
  {
    return query;
  }

  /**
   * Return this query as some kind of Druid query. The returned query will either be {@link TopNQuery},
   * {@link TimeseriesQuery}, {@link GroupByQuery}, {@link ScanQuery}, or {@link SelectQuery}.
   *
   * @return Druid query
   */
  private Query computeQuery()
  {
    final TimeseriesQuery tsQuery = toTimeseriesQuery();
    if (tsQuery != null) {
      return tsQuery;
    }

    final TopNQuery topNQuery = toTopNQuery();
    if (topNQuery != null) {
      return topNQuery;
    }

    final GroupByQuery groupByQuery = toGroupByQuery();
    if (groupByQuery != null) {
      return groupByQuery;
    }

    final StreamRawQuery scanQuery = toScanQuery();
    if (scanQuery != null) {
      return scanQuery;
    }

    throw new CannotBuildQueryException("Cannot convert query parts into an actual query");
  }

  /**
   * Return this query as a Timeseries query, or null if this query is not compatible with Timeseries.
   *
   * @return query
   */
  @Nullable
  public TimeseriesQuery toTimeseriesQuery()
  {
    if (grouping == null || grouping.getDimensions().size() > 1) {
      return null;
    }
    boolean descending = false;
    Granularity queryGranularity = Granularities.ALL;
    DimensionExpression dimension = Iterables.getOnlyElement(grouping.getDimensions(), null);
    if (dimension != null) {
      queryGranularity = Expressions.asGranularity(dimension.getDruidExpression());
      if (queryGranularity == null) {
        // Timeseries only applies if the single dimension is granular __time.
        return null;
      }
      if (limitSpec != null && !limitSpec.getColumns().isEmpty()) {
        List<OrderByColumnSpec> columns = limitSpec.getColumns();
        // We're ok if the first order by is time (since every time value is distinct, the rest of the columns
        // wouldn't matter anyway).
        OrderByColumnSpec firstOrderBy = columns.get(0);
        if (dimension.getOutputName().equals(firstOrderBy.getDimension())) {
          descending = firstOrderBy.getDirection() == Direction.DESCENDING;
        }
      }
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);

    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sortProject != null) {
      postAggregators.addAll(sortProject.getPostAggregators());
    }
    if (dimension != null) {
      DruidExpression expression = dimension.getDruidExpression();
      PostAggregator postAggregator;
      if (expression.isSimpleExtraction()) {
        postAggregator = new FieldAccessPostAggregator(dimension.getOutputName(), expression.getDirectColumn());
      } else {
        postAggregator = new MathPostAggregator(dimension.getOutputName(), expression.getExpression());
      }
      postAggregators.add(postAggregator);
    }

    final Map<String, Object> theContext = new HashMap<>();
    theContext.put("skipEmptyBuckets", true);
    theContext.putAll(plannerContext.getQueryContext());

    return new TimeseriesQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        filtration.getDimFilter(),
        queryGranularity,
        getVirtualColumns(false),
        grouping.getAggregatorFactories(),
        postAggregators,
        grouping.getHavingFilter(),
        limitSpec,
        null,
        null,
        ImmutableSortedMap.copyOf(theContext)
    );
  }

  /**
   * Return this query as a TopN query, or null if this query is not compatible with TopN.
   *
   * @return query or null
   */
  @Nullable
  public TopNQuery toTopNQuery()
  {
    // Must have GROUP BY one column, ORDER BY zero or one column, limit less than maxTopNLimit, and no HAVING.
    final boolean topNOk = grouping != null
                           && grouping.getDimensions().size() == 1
                           && limitSpec != null
                           && (limitSpec.getColumns().size() <= 1
                               && limitSpec.getLimit() <= plannerContext.getPlannerConfig().getMaxTopNLimit())
                           && grouping.getHavingFilter() == null;

    if (!topNOk) {
      return null;
    }

    final DimensionExpression dimensionExpr = Iterables.getOnlyElement(grouping.getDimensions());
    final DimensionSpec dimensionSpec = dimensionExpr.toDimensionSpec();
    if (!dimensionSpec.getDimension().equals(Row.TIME_COLUMN_NAME) &&
        !ValueDesc.isDimension(sourceRowSignature.getColumnType(dimensionSpec.getDimension()))) {
      return null;
    }
    final OrderByColumnSpec limitColumn;
    if (limitSpec.getColumns().isEmpty()) {
      limitColumn = new OrderByColumnSpec(
          dimensionSpec.getOutputName(),
          Direction.ASCENDING,
          Calcites.getStringComparatorForValueType(dimensionExpr.getOutputType())
      );
    } else {
      limitColumn = Iterables.getOnlyElement(limitSpec.getColumns());
    }
    final TopNMetricSpec topNMetricSpec;

    if (limitColumn.getDimension().equals(dimensionSpec.getOutputName())) {
      // DimensionTopNMetricSpec is exact; always return it even if allowApproximate is false.
      final DimensionTopNMetricSpec baseMetricSpec = new DimensionTopNMetricSpec(
          null,
          limitColumn.getDimensionOrder()
      );
      topNMetricSpec = limitColumn.getDirection() == Direction.ASCENDING
                       ? baseMetricSpec
                       : new InvertedTopNMetricSpec(baseMetricSpec);
    } else if (plannerContext.getPlannerConfig().isUseApproximateTopN()) {
      // ORDER BY metric
      final NumericTopNMetricSpec baseMetricSpec = new NumericTopNMetricSpec(limitColumn.getDimension());
      topNMetricSpec = limitColumn.getDirection() == Direction.ASCENDING
                       ? new InvertedTopNMetricSpec(baseMetricSpec)
                       : baseMetricSpec;
    } else {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);

    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sortProject != null) {
      postAggregators.addAll(sortProject.getPostAggregators());
    }

    return new TopNQuery(
        dataSource,
        getVirtualColumns(true),
        dimensionSpec,
        topNMetricSpec,
        limitSpec.getLimit(),
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        Granularities.ALL,
        grouping.getAggregatorFactories(),
        postAggregators,
        null,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }

  /**
   * Return this query as a GroupBy query, or null if this query is not compatible with GroupBy.
   *
   * @return query or null
   */
  @Nullable
  public GroupByQuery toGroupByQuery()
  {
    if (grouping == null) {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);

    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sortProject != null) {
      postAggregators.addAll(sortProject.getPostAggregators());
    }

    return new GroupByQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        Granularities.ALL,
        grouping.getDimensionSpecs(),
        null,
        getVirtualColumns(true),
        grouping.getAggregatorFactories(),
        postAggregators,
        grouping.getHavingFilter(),
        limitSpec,
        null,
        null,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }

  /**
   * Return this query as a Scan query, or null if this query is not compatible with Scan.
   *
   * @return query or null
   */
  @Nullable
  public StreamRawQuery toScanQuery()
  {
    if (grouping != null) {
      // Scan cannot GROUP BY.
      return null;
    }

    final List<String> rowOrder = outputRowSignature.getRowOrder();
    if (rowOrder.isEmpty()) {
      // Should never do a scan query without any columns that we're interested in. This is probably a planner bug.
      throw new ISE("WTF?! Attempting to convert to Scan query without any columns?");
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);
    final List<String> columns = Lists.newArrayList(rowOrder);
    Collections.sort(columns);

    // DefaultLimitSpec (which we use to "remember" limits) is int typed, and Integer.MAX_VALUE means "no limit".
    final int scanLimit = limitSpec == null || !limitSpec.hasLimit() ? 0 : limitSpec.getLimit();

    final List<OrderByColumnSpec> ordering = limitSpec == null ? null : limitSpec.getColumns();
    boolean descending = false;
    if (!GuavaUtils.isNullOrEmpty(ordering) && ordering.get(0).equals(OrderByColumnSpec.desc(Row.TIME_COLUMN_NAME))) {
      descending = true;
    }
    return new StreamRawQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        filtration.getDimFilter(),
        columns,
        selectProjection != null ? selectProjection.getVirtualColumns() : null,
        null,
        ordering,
        scanLimit,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }
}