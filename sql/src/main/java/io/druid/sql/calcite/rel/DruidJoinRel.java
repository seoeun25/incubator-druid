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
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.query.Druids;
import io.druid.query.JoinElement;
import io.druid.query.JoinType;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class DruidJoinRel extends DruidRel<DruidJoinRel> implements DruidRel.LeafRel
{
  private static final Logger LOG = new Logger(DruidJoinRel.class);

  public static DruidJoinRel create(Join join, JoinInfo joinInfo, DruidRel left, DruidRel right)
  {
    return new DruidJoinRel(
        left.getCluster(),
        left.getTraitSet(),
        join.getJoinType(),
        left,
        right,
        joinInfo.leftKeys,
        joinInfo.rightKeys,
        left.getQueryMaker()
    );
  }

  private final JoinRelType joinType;
  private final ImmutableIntList leftExpressions;
  private final ImmutableIntList rightExpressions;

  private RelNode left;
  private RelNode right;

  private DruidJoinRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final JoinRelType joinType,
      final RelNode left,
      final RelNode right,
      final ImmutableIntList leftExpressions,
      final ImmutableIntList rightExpressions,
      final QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.leftExpressions = leftExpressions;
    this.rightExpressions = rightExpressions;
  }

  @Override
  public DruidJoinRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    throw new UnsupportedOperationException("withPartialQuery");
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    final RelDataType rowType = getRowType();
    final DruidRel leftRel = Utils.getDruidRel(left);
    final DruidRel rightRel = Utils.getDruidRel(right);
    if (leftRel == null || rightRel == null) {
      return null;
    }
    final DruidQuery leftQuery = leftRel.toDruidQuery(true);
    final DruidQuery rightQuery = rightRel.toDruidQuery(true);
    if (leftQuery == null || rightQuery == null) {
      return null;
    }
    final List<String> leftOrder = leftQuery.getOutputRowSignature().getRowOrder();
    final List<String> rightOrder = rightQuery.getOutputRowSignature().getRowOrder();

    final RowSignature outRowSignature = RowSignature.from(Queries.uniqueNames(leftOrder, rightOrder), rowType);

    final List<String> leftKeys = Lists.newArrayList();
    for (int leftKey : leftExpressions) {
      leftKeys.add(leftOrder.get(leftKey));
    }
    final List<String> rightKeys = Lists.newArrayList();
    for (int rightKey : rightExpressions) {
      rightKeys.add(rightOrder.get(rightKey));
    }

    return new DruidQuery()
    {
      @Override
      public RelDataType getOutputRowType()
      {
        return rowType;
      }

      @Override
      public RowSignature getOutputRowSignature()
      {
        return outRowSignature;
      }

      @Override
      public Query getQuery()
      {
        final Query leftDruid = leftQuery.getQuery();
        final Query rightDruid = rightQuery.getQuery();
        final String leftAlias = toAlias(leftDruid);
        String rightAlias = toAlias(rightDruid);
        while (leftAlias.equals(rightAlias)) {
          rightAlias += "$";
        }
        return new Druids.JoinQueryBuilder()
            .dataSource(leftAlias, QueryDataSource.of(leftDruid))
            .dataSource(rightAlias, QueryDataSource.of(rightDruid))
            .element(new JoinElement(JoinType.fromString(joinType.name()), leftAlias, leftKeys, rightAlias, rightKeys))
            .asArray(true)
            .build();
      }
    };
  }

  private String toAlias(Query query)
  {
    return StringUtils.join(query.getDataSource().getNames(), '+');
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return toDruidQuery(false);
  }

  @Override
  public DruidJoinRel asBindable()
  {
    return new DruidJoinRel(
        getCluster(),
        getTraitSet().replace(BindableConvention.INSTANCE),
        joinType,
        RelOptRule.convert(left, BindableConvention.INSTANCE),
        RelOptRule.convert(right, BindableConvention.INSTANCE),
        leftExpressions,
        rightExpressions,
        getQueryMaker()
    );
  }

  @Override
  public DruidJoinRel asDruidConvention()
  {
    return new DruidJoinRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        joinType,
        RelOptRule.convert(left, DruidConvention.instance()),
        RelOptRule.convert(right, DruidConvention.instance()),
        leftExpressions,
        rightExpressions,
        getQueryMaker()
    );
  }

  @Override
  public int getQueryCount()
  {
    DruidRel leftRel = Preconditions.checkNotNull(Utils.getDruidRel(left));
    DruidRel rightRel = Preconditions.checkNotNull(Utils.getDruidRel(right));
    return leftRel.getQueryCount() + rightRel.getQueryCount();
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return SqlValidatorUtil.deriveJoinRowType(
        left.getRowType(),
        right.getRowType(),
        joinType,
        getCluster().getTypeFactory(),
        null,
        ImmutableList.of()
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    switch (ordinalInParent) {
      case 0:
        this.left = p;
        break;
      case 1:
        this.right = p;
        break;
      default:
        throw new IndexOutOfBoundsException("Input " + ordinalInParent);
    }
    recomputeDigest();
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidJoinRel(
        getCluster(),
        traitSet,
        joinType,
        inputs.get(0),
        inputs.get(1),
        leftExpressions,
        rightExpressions,
        getQueryMaker()
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw)
                .input("left", left)
                .input("right", right)
                .item("joinType", joinType)
                .item("leftExpressions", leftExpressions)
                .item("rightExpressions", rightExpressions);
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    final RelOptCostFactory costFactory = planner.getCostFactory();
    if (leftExpressions.isEmpty()) {
      return costFactory.makeCost((1 + mq.getRowCount(left)) * (1 + mq.getRowCount(right)) * 20, 0, 0);
    }
    return costFactory.makeCost(mq.getRowCount(left) + mq.getRowCount(right), 0, 0);
  }
}