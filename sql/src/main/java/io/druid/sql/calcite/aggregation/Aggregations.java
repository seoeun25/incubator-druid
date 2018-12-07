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

package io.druid.sql.calcite.aggregation;

import io.druid.data.ValueDesc;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Aggregations
{
  private Aggregations()
  {
    // No instantiation.
  }

  @Nullable
  public static List<DruidExpression> getArgumentsForSimpleAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final AggregateCall call,
      final Project project
  )
  {
    return call.getArgList().stream()
               .map(i -> Expressions.fromFieldAccess(rowSignature, project, i))
               .map(rexNode -> toDruidExpressionForSimpleAggregator(plannerContext, rowSignature, rexNode))
               .collect(Collectors.toList());
  }

  private static DruidExpression toDruidExpressionForSimpleAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final DruidExpression druidExpression = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
    if (druidExpression == null) {
      return null;
    }

    if (druidExpression.isSimpleExtraction() &&
        (!druidExpression.isDirectColumnAccess()
         || ValueDesc.isStringOrDimension(rowSignature.getColumnType(druidExpression.getDirectColumn())))) {
      // Aggregators are unable to implicitly cast strings to numbers. So remove the simple extraction in this case.
      return druidExpression.map(simpleExtraction -> null, Function.identity());
    } else {
      return druidExpression;
    }
  }
}
