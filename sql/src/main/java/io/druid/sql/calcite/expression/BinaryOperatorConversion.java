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

package io.druid.sql.calcite.expression;

import com.google.common.base.Joiner;
import com.metamx.common.ISE;
import io.druid.common.utils.StringUtils;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;

import java.util.stream.Collectors;

public class BinaryOperatorConversion implements SqlOperatorConversion
{
  private final SqlOperator operator;
  private final Joiner joiner;

  public BinaryOperatorConversion(final SqlOperator operator, final String druidOperator)
  {
    this.operator = operator;
    this.joiner = Joiner.on(" " + druidOperator + " ");
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    return OperatorConversions.convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        operands -> {
          if (operands.size() < 2) {
            throw new ISE("Got binary operator[%s] with %s args?", operator.getName(), operands.size());
          }

          return DruidExpression.fromExpression(
              StringUtils.format(
                  "(%s)",
                  joiner.join(
                      operands.stream()
                              .map(DruidExpression::getExpression)
                              .collect(Collectors.toList())
                  )
              )
          );
        }
    );
  }
}
