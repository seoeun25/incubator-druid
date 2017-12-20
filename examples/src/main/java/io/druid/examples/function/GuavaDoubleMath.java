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

package io.druid.examples.function;

import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;
import io.druid.math.expr.BuiltinFunctions.SingleParamMath;
import io.druid.math.expr.BuiltinFunctions.TripleParamMath;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprType;
import io.druid.math.expr.Function;

import java.util.List;

/**
 */
public class GuavaDoubleMath implements Function.Library
{
  @Function.Named("factorial")
  public static class Factorial extends SingleParamMath
  {
    @Override
    public ExprType type(ExprType param)
    {
      return ExprType.DOUBLE;
    }

    @Override
    protected ExprEval eval(long param)
    {
      return ExprEval.of(DoubleMath.factorial(Ints.checkedCast(param)));
    }
  }

  @Function.Named("fuzzyCompare")
  public static class FuzzyCompare extends TripleParamMath
  {
    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.DOUBLE;
    }

    @Override
    protected ExprEval eval(double x, double y, double z)
    {
      return ExprEval.of(DoubleMath.fuzzyCompare(x, y, z));
    }
  }
}
